from __future__ import annotations

from dataclasses import asdict
from typing import Iterable, Iterator, Optional

from ..models import Event, EventFile, Particle, RunInfo, Vertex
from ..provenance import stable_json_dumps
from .reader_base import Reader
from .writer_base import Writer


def _require_pyarrow():
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception as e:  # pragma: no cover
        raise ImportError("Parquet support requires 'pyarrow'. Install hepconduit[parquet].") from e
    return pa, pq


_META_PREFIX = "hepconduit."


def _md_get(md: dict[str, str], key: str, default=None):
    return md.get(f"{_META_PREFIX}{key}", md.get(key, default))


def _md_set(md: dict[str, str], key: str, value) -> None:
    md[f"{_META_PREFIX}{key}"] = str(value)


def json_loads_lenient(s: str):
    import json
    try:
        return json.loads(s)
    except Exception:
        try:
            return json.loads(s.strip('"'))
        except Exception:
            return None


def _encode_run_info(run_info: Optional[RunInfo]) -> dict[str, str]:
    run_info = run_info or RunInfo()
    md: dict[str, str] = {}
    _md_set(md, "run_info_json", stable_json_dumps(asdict(run_info)))
    _md_set(md, "beam_pdg_id", stable_json_dumps(list(run_info.beam_pdg_id)))
    _md_set(md, "beam_energy", stable_json_dumps(list(run_info.beam_energy)))
    _md_set(md, "weight_names", stable_json_dumps(list(run_info.weight_names)))
    _md_set(md, "generator_name", run_info.generator_name or "")
    _md_set(md, "generator_version", run_info.generator_version or "")
    units = (run_info.extra or {}).get("units")
    if units is not None:
        _md_set(md, "units", stable_json_dumps(units))
    return md


def _decode_run_info(md: dict[str, str]) -> RunInfo:
    raw = _md_get(md, "run_info_json")
    if not raw:
        return RunInfo(extra={"parquet_schema_metadata": md})
    d = json_loads_lenient(raw)
    if not isinstance(d, dict):
        return RunInfo(extra={"parquet_schema_metadata": md})
    ri = RunInfo()
    for k, v in d.items():
        try:
            setattr(ri, k, v)
        except Exception:
            pass
    ri.extra = dict(getattr(ri, "extra", {}) or {})
    ri.extra.setdefault("parquet_schema_metadata", md)
    return ri


def _vertex_lookup(vertices: list[Vertex]) -> dict[int, Vertex]:
    return {int(v.barcode): v for v in (vertices or [])}


def _ensure_vertices_from_particles(particles: list[Particle], vertices: list[Vertex]) -> list[Vertex]:
    vmap = _vertex_lookup(vertices or [])
    for p in particles:
        for vb in (p.vertex_barcode, p.end_vertex_barcode):
            if vb and vb not in vmap:
                vmap[vb] = Vertex(barcode=int(vb))
    for v in vmap.values():
        v.incoming = []
        v.outgoing = []
    for p in particles:
        if p.vertex_barcode:
            vmap[int(p.vertex_barcode)].outgoing.append(int(p.barcode))
        if p.end_vertex_barcode:
            vmap[int(p.end_vertex_barcode)].incoming.append(int(p.barcode))
    for v in vmap.values():
        v.incoming = sorted(set(v.incoming))
        v.outgoing = sorted(set(v.outgoing))
    return [vmap[k] for k in sorted(vmap.keys())]


def _build_vertices_from_mothers(ev: Event) -> None:
    if ev.vertices:
        return

    for i, p in enumerate(ev.particles, start=1):
        if not p.barcode:
            p.barcode = i

    vtx_map: dict[tuple[int, int], int] = {}
    vertices: dict[int, Vertex] = {}
    next_vtx = -1

    def _vtx_for(m1: int, m2: int) -> int:
        nonlocal next_vtx
        if m1 == 0 and m2 == 0:
            key = (0, 0)
        else:
            a, b = (m1, m2) if m1 <= m2 else (m2, m1)
            key = (a, b)
        if key not in vtx_map:
            vtx_map[key] = next_vtx
            vertices[next_vtx] = Vertex(barcode=next_vtx)
            next_vtx -= 1
        return vtx_map[key]

    for p in ev.particles:
        m1, m2 = int(p.mother1 or 0), int(p.mother2 or 0)
        vtx = _vtx_for(m1, m2)
        p.vertex_barcode = vtx
        vertices[vtx].outgoing.append(int(p.barcode))

    for p in ev.particles:
        vtx = int(p.vertex_barcode or 0)
        if vtx == 0:
            continue
        m1, m2 = int(p.mother1 or 0), int(p.mother2 or 0)
        for midx in (m1, m2):
            if midx and 1 <= midx <= len(ev.particles):
                mother = ev.particles[midx - 1]
                vertices[vtx].incoming.append(int(mother.barcode))
                mother.end_vertex_barcode = vtx

    for v in vertices.values():
        v.incoming = sorted(set(v.incoming))
        v.outgoing = sorted(set(v.outgoing))

    ev.vertices = [vertices[k] for k in sorted(vertices.keys())]


class ParquetReader(Reader):
    def read(self, path: str) -> EventFile:
        pa, pq = _require_pyarrow()
        table = pq.read_table(path)
        md: dict[str, str] = {}
        try:
            if table.schema.metadata:
                for k, v in table.schema.metadata.items():
                    md[k.decode("utf-8", "replace")] = v.decode("utf-8", "replace")
        except Exception:
            md = {}

        run_info = _decode_run_info(md)
        cols = set(table.column_names)

        if "particles" in cols:
            ef = _read_columnar(table, run_info=run_info)
        else:
            ef = _read_flat(table, run_info=run_info)

        ef.format_name = "parquet"
        return ef

    def iter_events(self, path: str) -> Iterator[Event]:
        return iter(self.read(path).events)


def _read_flat(table, *, run_info: RunInfo) -> EventFile:
    from collections import defaultdict

    cols = set(table.column_names)
    ev_col = table["event_number"].to_pylist()

    def col(name, default=None):
        if name in cols:
            return table[name].to_pylist()
        return [default] * len(ev_col)

    pdg = col("pdg_id", 0)
    status = col("status", 0)
    px = col("px", 0.0)
    py = col("py", 0.0)
    pz = col("pz", 0.0)
    e = col("energy", 0.0)
    m = col("mass", 0.0)
    mother1 = col("mother1", 0)
    mother2 = col("mother2", 0)
    color1 = col("color1", 0)
    color2 = col("color2", 0)
    spin = col("spin", 9.0)
    barcode = col("barcode", 0)
    vbar = col("vertex_barcode", 0)
    evbar = col("end_vertex_barcode", 0)
    attr_json = col("attributes_json", None)

    proc = col("process_id", 0)
    scale = col("scale", 0.0)
    aqed = col("alpha_qed", 0.0)
    aqcd = col("alpha_qcd", 0.0)
    weights = col("weights", None)
    extra_json = col("event_extra_json", None)

    have_vpos = {"prod_vx", "prod_vy", "prod_vz", "prod_vt", "end_vx", "end_vy", "end_vz", "end_vt"} <= cols
    pvx = col("prod_vx", 0.0)
    pvy = col("prod_vy", 0.0)
    pvz = col("prod_vz", 0.0)
    pvt = col("prod_vt", 0.0)
    evx = col("end_vx", 0.0)
    evy = col("end_vy", 0.0)
    evz = col("end_vz", 0.0)
    evt = col("end_vt", 0.0)

    by_ev = defaultdict(list)
    ev_meta: dict[int, dict] = {}
    vpos_by_ev: dict[int, dict[int, tuple[float, float, float, float]]] = {}

    for i, evn in enumerate(ev_col):
        evn_i = int(evn)
        attrs = json_loads_lenient(attr_json[i]) if (attr_json and attr_json[i]) else {}
        p = Particle(
            pdg_id=int(pdg[i]),
            status=int(status[i]),
            px=float(px[i]),
            py=float(py[i]),
            pz=float(pz[i]),
            energy=float(e[i]),
            mass=float(m[i]),
            mother1=int(mother1[i]),
            mother2=int(mother2[i]),
            color1=int(color1[i]),
            color2=int(color2[i]),
            spin=float(spin[i]) if spin is not None else 9.0,
            barcode=int(barcode[i]) if barcode is not None else 0,
            vertex_barcode=int(vbar[i]) if vbar is not None else 0,
            end_vertex_barcode=int(evbar[i]) if evbar is not None else 0,
            attributes=attrs or {},
        )
        by_ev[evn_i].append(p)

        if evn_i not in ev_meta:
            ev_meta[evn_i] = {
                "weights": [float(x) for x in (weights[i] or [1.0])] if weights else [1.0],
                "process_id": int(proc[i] or 0),
                "scale": float(scale[i] or 0.0),
                "alpha_qed": float(aqed[i] or 0.0),
                "alpha_qcd": float(aqcd[i] or 0.0),
                "extra": json_loads_lenient(extra_json[i]) if (extra_json and extra_json[i]) else {},
            }

        if have_vpos:
            vpos_by_ev.setdefault(evn_i, {})
            if p.vertex_barcode:
                vpos_by_ev[evn_i][int(p.vertex_barcode)] = (float(pvx[i]), float(pvy[i]), float(pvz[i]), float(pvt[i]))
            if p.end_vertex_barcode:
                vpos_by_ev[evn_i][int(p.end_vertex_barcode)] = (float(evx[i]), float(evy[i]), float(evz[i]), float(evt[i]))

    events: list[Event] = []
    for evn, parts in sorted(by_ev.items()):
        meta = ev_meta.get(evn, {})
        ev = Event(
            event_number=evn,
            particles=parts,
            n_particles=len(parts),
            weights=meta.get("weights", [1.0]),
            process_id=meta.get("process_id", 0),
            scale=meta.get("scale", 0.0),
            alpha_qed=meta.get("alpha_qed", 0.0),
            alpha_qcd=meta.get("alpha_qcd", 0.0),
            extra=meta.get("extra", {}) or {},
        )

        vertices: list[Vertex] = []
        if have_vpos:
            for vb, (x, y, z, t) in (vpos_by_ev.get(evn, {}) or {}).items():
                vertices.append(Vertex(barcode=int(vb), x=float(x), y=float(y), z=float(z), t=float(t)))

        if not vertices:
            _build_vertices_from_mothers(ev)
        else:
            ev.vertices = _ensure_vertices_from_particles(ev.particles, vertices)

        events.append(ev)

    return EventFile(run_info=run_info, events=events, format_name="parquet")


def _read_columnar(table, *, run_info: RunInfo) -> EventFile:
    cols = set(table.column_names)
    ev_numbers = table["event_number"].to_pylist() if "event_number" in cols else list(range(1, table.num_rows + 1))

    def col(name, default=None):
        if name in cols:
            return table[name].to_pylist()
        return [default] * len(ev_numbers)

    proc = col("process_id", 0)
    scale = col("scale", 0.0)
    aqed = col("alpha_qed", 0.0)
    aqcd = col("alpha_qcd", 0.0)
    weights = col("weights", None)
    particles_col = col("particles", None)
    vertices_col = col("vertices", None)
    extra_json = col("event_extra_json", None)

    events: list[Event] = []
    for i, plist in enumerate(particles_col):
        parts: list[Particle] = []
        for p in (plist or []):
            attrs = p.get("attributes")
            if attrs is None and p.get("attributes_json"):
                attrs = json_loads_lenient(p.get("attributes_json")) or {}
            parts.append(
                Particle(
                    pdg_id=int(p.get("pdg_id", 0)),
                    status=int(p.get("status", 0)),
                    px=float(p.get("px", 0.0)),
                    py=float(p.get("py", 0.0)),
                    pz=float(p.get("pz", 0.0)),
                    energy=float(p.get("energy", 0.0)),
                    mass=float(p.get("mass", 0.0)),
                    mother1=int(p.get("mother1", 0)),
                    mother2=int(p.get("mother2", 0)),
                    color1=int(p.get("color1", 0)),
                    color2=int(p.get("color2", 0)),
                    spin=float(p.get("spin", 9.0)),
                    barcode=int(p.get("barcode", 0)),
                    vertex_barcode=int(p.get("vertex_barcode", 0)),
                    end_vertex_barcode=int(p.get("end_vertex_barcode", 0)),
                    attributes=attrs or {},
                )
            )

        verts: list[Vertex] = []
        for v in (vertices_col[i] or []):
            verts.append(
                Vertex(
                    barcode=int(v.get("barcode", 0)),
                    x=float(v.get("x", 0.0)),
                    y=float(v.get("y", 0.0)),
                    z=float(v.get("z", 0.0)),
                    t=float(v.get("t", 0.0)),
                    incoming=[int(x) for x in (v.get("incoming") or [])],
                    outgoing=[int(x) for x in (v.get("outgoing") or [])],
                )
            )

        ev = Event(
            event_number=int(ev_numbers[i]),
            particles=parts,
            vertices=_ensure_vertices_from_particles(parts, verts),
            n_particles=len(parts),
            weights=[float(x) for x in ((weights[i] or []) if weights else [1.0])],
            process_id=int(proc[i] or 0),
            scale=float(scale[i] or 0.0),
            alpha_qed=float(aqed[i] or 0.0),
            alpha_qcd=float(aqcd[i] or 0.0),
            extra=json_loads_lenient(extra_json[i]) if (extra_json and extra_json[i]) else {},
        )

        if not ev.vertices:
            _build_vertices_from_mothers(ev)

        events.append(ev)

    return EventFile(run_info=run_info, events=events, format_name="parquet")


class ParquetWriter(Writer):
    def write(self, path: str, events: Iterable[Event], run_info: Optional[RunInfo], **kwargs) -> None:
        columnar = bool(kwargs.get("columnar", False))
        metadata_in = kwargs.get("metadata") or {}
        pa, pq = _require_pyarrow()

        md = _encode_run_info(run_info)
        for k, v in metadata_in.items():
            md[str(k)] = str(v)

        if columnar:
            rows = []
            for ev in events:
                _build_vertices_from_mothers(ev)
                rows.append({
                    "event_number": ev.event_number,
                    "process_id": ev.process_id,
                    "scale": ev.scale,
                    "alpha_qed": ev.alpha_qed,
                    "alpha_qcd": ev.alpha_qcd,
                    "weights": list(ev.weights or [1.0]),
                    "event_extra_json": stable_json_dumps(ev.extra or {}),
                    "particles": [
                        {
                            "pdg_id": p.pdg_id,
                            "status": p.status,
                            "mother1": p.mother1,
                            "mother2": p.mother2,
                            "color1": p.color1,
                            "color2": p.color2,
                            "px": p.px,
                            "py": p.py,
                            "pz": p.pz,
                            "energy": p.energy,
                            "mass": p.mass,
                            "spin": p.spin,
                            "barcode": p.barcode,
                            "vertex_barcode": p.vertex_barcode,
                            "end_vertex_barcode": p.end_vertex_barcode,
                            "attributes_json": stable_json_dumps(p.attributes or {}),
                        }
                        for p in ev.particles
                    ],
                    "vertices": [
                        {
                            "barcode": v.barcode,
                            "x": v.x,
                            "y": v.y,
                            "z": v.z,
                            "t": v.t,
                            "incoming": list(v.incoming or []),
                            "outgoing": list(v.outgoing or []),
                        }
                        for v in (ev.vertices or [])
                    ],
                })
            table = pa.Table.from_pylist(rows)
            table = table.replace_schema_metadata({k: str(v) for k, v in md.items()})
            pq.write_table(table, path)
            return

        rows = []
        for ev in events:
            _build_vertices_from_mothers(ev)
            vmap = _vertex_lookup(ev.vertices or [])
            for p in ev.particles:
                prod = vmap.get(int(p.vertex_barcode)) if p.vertex_barcode else None
                endv = vmap.get(int(p.end_vertex_barcode)) if p.end_vertex_barcode else None
                rows.append({
                    "event_number": ev.event_number,
                    "process_id": ev.process_id,
                    "scale": ev.scale,
                    "alpha_qed": ev.alpha_qed,
                    "alpha_qcd": ev.alpha_qcd,
                    "weights": list(ev.weights or [1.0]),
                    "event_extra_json": stable_json_dumps(ev.extra or {}),
                    **p.to_dict(),
                    "attributes_json": stable_json_dumps(p.attributes or {}),
                    "prod_vx": float(prod.x) if prod else 0.0,
                    "prod_vy": float(prod.y) if prod else 0.0,
                    "prod_vz": float(prod.z) if prod else 0.0,
                    "prod_vt": float(prod.t) if prod else 0.0,
                    "end_vx": float(endv.x) if endv else 0.0,
                    "end_vy": float(endv.y) if endv else 0.0,
                    "end_vz": float(endv.z) if endv else 0.0,
                    "end_vt": float(endv.t) if endv else 0.0,
                })
        table = pa.Table.from_pylist(rows)
        table = table.replace_schema_metadata({k: str(v) for k, v in md.items()})
        pq.write_table(table, path)
