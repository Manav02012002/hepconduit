from __future__ import annotations

import gzip
import io
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from ..models import Event, EventFile, Particle, RunInfo, Vertex
from .reader_base import Reader
from .writer_base import Writer


def _open_text(path: str):
    p = Path(path)
    if p.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(p, "rb"), encoding="utf-8", errors="replace")
    return open(p, "r", encoding="utf-8", errors="replace")


# --- HepMC3 Asciiv3 support (production-grade subset) ---------------------------------
#
# We implement the commonly-used HepMC3 Asciiv3 record types:
#   HepMC::Version / HepMC::Asciiv3 headers
#   U <mom_unit> <len_unit>
#   N <n> <name1> <name2> ...            (run-level weight names)
#   E <evtno> [<attrs...>]               (event start)
#   W <w1> <w2> ...                      (event weights)
#   V <vtxid> <x> <y> <z> <t> [<nin> <nout> <in...> <out...>]  (vertex)
#   P <pid> <pdg> <status> <px> <py> <pz> <e> <m> [<pv> <ev>]   (particle)
#
# Unknown record types are preserved in RunInfo.extra / Event.extra when possible.
# The parser is robust: it ignores extra columns while capturing the canonical ones.


def iter_hepmc3(path: str) -> Iterator[Event]:
    """Iterate events from a HepMC3 ASCII (Asciiv3) file."""
    _runinfo, it = iter_hepmc3_with_runinfo(path)
    yield from it


def iter_hepmc3_with_runinfo(path: str) -> Tuple[RunInfo, Iterator[Event]]:
    run = RunInfo()
    run.extra.setdefault("hepmc3", {})
    run.extra["hepmc3"].setdefault("raw_headers", [])

    def _events() -> Iterator[Event]:
        with _open_text(path) as f:
            current: Optional[Event] = None
            vertices: Dict[int, Vertex] = {}
            particles_by_bc: Dict[int, Particle] = {}

            mom_unit = None
            len_unit = None

            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                # Headers / run info
                if line.startswith("HepMC::"):
                    run.extra["hepmc3"]["raw_headers"].append(line)
                    continue

                tag = line.split(maxsplit=1)[0]

                if tag == "U":
                    parts = line.split()
                    if len(parts) >= 3:
                        mom_unit, len_unit = parts[1], parts[2]
                        run.extra["units"] = {"momentum": mom_unit, "length": len_unit}
                    else:
                        run.extra["units"] = {}
                    continue

                if tag == "N":
                    parts = line.split()
                    # N <n> <name1> ...
                    if len(parts) >= 2:
                        try:
                            n = int(parts[1])
                        except Exception:
                            n = 0
                        names = parts[2:]
                        if n and len(names) >= n:
                            run.weight_names = names[:n]
                        else:
                            run.weight_names = names
                    continue

                if tag == "F":
                    # GenRunInfo line; preserve
                    run.extra["hepmc3"].setdefault("F", []).append(line)
                    continue

                if tag == "C":
                    # Cross section info line; preserve
                    run.extra["hepmc3"].setdefault("C", []).append(line)
                    continue

                # Event start
                if tag == "E":
                    if current is not None:
                        # finalize last event: attach collected vertices (sorted by barcode for determinism)
                        current.vertices = [vertices[k] for k in sorted(vertices.keys())]
                        yield current
                    parts = line.split()
                    evtno = 0
                    if len(parts) >= 2:
                        try:
                            evtno = int(parts[1])
                        except Exception:
                            evtno = 0
                    current = Event(event_number=evtno)
                    current.extra.setdefault("hepmc3", {})
                    current.extra["hepmc3"]["E_raw"] = line
                    vertices = {}
                    particles_by_bc = {}
                    continue

                if current is None:
                    # Skip stray records before first event
                    continue

                if tag == "W":
                    parts = line.split()
                    ws: List[float] = []
                    for tok in parts[1:]:
                        try:
                            ws.append(float(tok))
                        except Exception:
                            pass
                    if ws:
                        current.weights = ws
                    continue

                if tag == "V":
                    parts = line.split()
                    # V <vtxid> <x> <y> <z> <t> [<nin> <nout> <in...> <out...>]
                    if len(parts) < 6:
                        continue
                    try:
                        vtxid = int(parts[1])
                        x, y, z, t = float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                    except Exception:
                        continue
                    v = Vertex(barcode=vtxid, x=x, y=y, z=z, t=t)
                    idx = 6
                    if len(parts) >= idx + 2:
                        try:
                            nin = int(parts[idx])
                            nout = int(parts[idx + 1])
                            idx += 2
                            incoming = []
                            outgoing = []
                            for _ in range(nin):
                                if idx >= len(parts):
                                    break
                                try:
                                    incoming.append(int(parts[idx]))
                                except Exception:
                                    pass
                                idx += 1
                            for _ in range(nout):
                                if idx >= len(parts):
                                    break
                                try:
                                    outgoing.append(int(parts[idx]))
                                except Exception:
                                    pass
                                idx += 1
                            v.incoming = incoming
                            v.outgoing = outgoing
                        except Exception:
                            pass
                    vertices[vtxid] = v
                    continue

                if tag == "P":
                    parts = line.split()
                    # P <bc> <pdg> <status> <px> <py> <pz> <e> <m> [<pv> <ev>]
                    if len(parts) < 9:
                        continue
                    try:
                        bc = int(parts[1])
                        pdg = int(parts[2])
                        st = int(parts[3])
                        px, py, pz = float(parts[4]), float(parts[5]), float(parts[6])
                        e, m = float(parts[7]), float(parts[8])
                    except Exception:
                        continue

                    pv = 0
                    ev = 0
                    if len(parts) >= 11:
                        try:
                            pv = int(parts[9])
                            ev = int(parts[10])
                        except Exception:
                            pv = 0
                            ev = 0

                    # Map HepMC-like statuses into internal convention when possible.
                    mapped_status = st
                    if st in (4,):  # beam
                        mapped_status = -1
                    elif st in (1,):
                        mapped_status = 1
                    elif st in (2, 3):
                        mapped_status = 2

                    p = Particle(
                        pdg_id=pdg,
                        status=mapped_status,
                        px=px,
                        py=py,
                        pz=pz,
                        energy=e,
                        mass=m,
                        barcode=bc,
                        vertex_barcode=pv,
                        end_vertex_barcode=ev,
                    )
                    # Preserve original status if it doesn't map cleanly
                    if mapped_status != st:
                        p.attributes["hepmc_status_raw"] = st

                    particles_by_bc[bc] = p
                    current.particles.append(p)
                    continue

                # Preserve unknown tags at event scope
                current.extra.setdefault("hepmc3", {}).setdefault("unknown_records", []).append(line)

            if current is not None:
                current.vertices = [vertices[k] for k in sorted(vertices.keys())]
                yield current

    return run, _events()


def read_hepmc3(path: str) -> EventFile:
    run, it = iter_hepmc3_with_runinfo(path)
    events = list(it)
    return EventFile(run_info=run, events=events, format_name="hepmc3")


class HepMC3Reader(Reader):
    def iter_events(self, path: str) -> Iterator[Event]:
        return iter_hepmc3(path)

    def read(self, path: str) -> EventFile:
        return read_hepmc3(path)


# --- Vertex reconstruction for formats without explicit graph ---------------------------


def _build_vertices_from_mothers(ev: Event) -> None:
    """Construct a HepMC-style vertex graph from LHE-style mother indices.

    This gives a faithful interaction graph for decays/branchings encoded
    by mother1/mother2 in LHE, rather than emitting a 'flat list' record.
    """
    if ev.vertices:
        return

    # Assign stable particle barcodes if missing (1..N)
    for i, p in enumerate(ev.particles, start=1):
        if not p.barcode:
            p.barcode = i

    # Map mother-index pairs -> vertex id
    vtx_map: Dict[Tuple[int, int], int] = {}
    vertices: Dict[int, Vertex] = {}
    next_vtx = -1

    def _vtx_for(m1: int, m2: int) -> int:
        nonlocal next_vtx
        key = (m1, m2) if m1 <= m2 else (m2, m1)
        if key not in vtx_map:
            vtx_map[key] = next_vtx
            vertices[next_vtx] = Vertex(barcode=next_vtx)
            next_vtx -= 1
        return vtx_map[key]

    # Production vertex of each particle
    prod_vtx: Dict[int, int] = {}

    # Incoming particles have no production vertex
    for idx, p in enumerate(ev.particles, start=1):
        if p.status == -1:
            prod_vtx[idx] = 0
            p.vertex_barcode = 0

    # Create vertices for produced particles
    for idx, p in enumerate(ev.particles, start=1):
        if p.status == -1:
            continue
        m1, m2 = int(p.mother1), int(p.mother2)
        if m1 == 0 and m2 == 0:
            # If no mothers, attach to an implicit hard-scatter vertex keyed by (0,0)
            v_id = _vtx_for(0, 0)
        else:
            v_id = _vtx_for(m1, m2)
        prod_vtx[idx] = v_id
        p.vertex_barcode = v_id

    # Fill vertex incoming/outgoing using mother relationships
    for child_idx, p in enumerate(ev.particles, start=1):
        v_id = prod_vtx.get(child_idx, 0)
        if v_id == 0:
            continue
        v = vertices[v_id]
        # incoming: mothers (if present), outgoing: this child
        for midx in {int(p.mother1), int(p.mother2)} - {0}:
            if 1 <= midx <= len(ev.particles):
                mbar = ev.particles[midx - 1].barcode
                if mbar not in v.incoming:
                    v.incoming.append(mbar)
        cbar = p.barcode
        if cbar not in v.outgoing:
            v.outgoing.append(cbar)

    # End vertex for a particle is the vertex where it appears as incoming
    incoming_to_vtx: Dict[int, int] = {}
    for vid, v in vertices.items():
        for inc in v.incoming:
            incoming_to_vtx[inc] = vid

    for p in ev.particles:
        p.end_vertex_barcode = incoming_to_vtx.get(p.barcode, 0)

    ev.vertices = [vertices[k] for k in sorted(vertices.keys())]


class HepMC3Writer(Writer):
    def write(self, path: str, events: Iterable[Event], run_info: Optional[RunInfo], **kwargs) -> None:
        p = Path(path)
        if p.suffix == ".gz":
            raw = gzip.open(p, "wb")
            f = io.TextIOWrapper(raw, encoding="utf-8")
        else:
            f = open(p, "w", encoding="utf-8")
        with f:
            f.write("HepMC::Version 3.0.0\n")
            f.write("HepMC::Asciiv3\n")

            run = run_info or RunInfo()
            units = (run.extra or {}).get("units") if run else None
            if isinstance(units, dict) and units.get("momentum") and units.get("length"):
                f.write(f"U {units['momentum']} {units['length']}\n")
            else:
                # Default to common generator-level units
                f.write("U GEV MM\n")

            if run.weight_names:
                f.write("N {} {}\n".format(len(run.weight_names), " ".join(run.weight_names)))

            # Preserve some raw run header records if present
            hepmc_extra = (run.extra or {}).get("hepmc3", {}) or {}
            for line in hepmc_extra.get("F", []) or []:
                f.write(line.rstrip("\n") + "\n")
            for line in hepmc_extra.get("C", []) or []:
                f.write(line.rstrip("\n") + "\n")

            for ev in events:
                # Ensure we have a vertex graph (even when input came from LHE/CSV)
                _build_vertices_from_mothers(ev)

                # Deterministic E line: event number only (other fields may exist but are optional)
                f.write(f"E {ev.event_number}\n")

                # Weights
                if ev.weights and (len(ev.weights) > 1 or (len(ev.weights) == 1 and ev.weights[0] != 1.0)):
                    f.write("W {}\n".format(" ".join(f"{w:.17g}" for w in ev.weights)))

                # Vertices
                vtx_by_id = {v.barcode: v for v in ev.vertices}
                for vid in sorted(vtx_by_id.keys()):
                    v = vtx_by_id[vid]
                    # V <id> <x> <y> <z> <t> <nin> <nout> <in...> <out...>
                    f.write(
                        "V {id} {x:.17g} {y:.17g} {z:.17g} {t:.17g} {nin} {nout} {ins} {outs}\n".format(
                            id=v.barcode,
                            x=v.x,
                            y=v.y,
                            z=v.z,
                            t=v.t,
                            nin=len(v.incoming),
                            nout=len(v.outgoing),
                            ins=" ".join(str(i) for i in v.incoming) if v.incoming else "",
                            outs=" ".join(str(o) for o in v.outgoing) if v.outgoing else "",
                        )
                    )

                # Particles
                for i, part in enumerate(ev.particles, start=1):
                    bc = part.barcode if part.barcode else i
                    # Map back to HepMC status codes (best-effort)
                    out_status = part.status
                    if part.status == -1:
                        out_status = 4
                    elif part.status == 1:
                        out_status = 1
                    elif part.status == 2:
                        out_status = 2
                    # If we preserved a raw status, prefer it
                    raw_st = part.attributes.get("hepmc_status_raw") if isinstance(part.attributes, dict) else None
                    if isinstance(raw_st, int):
                        out_status = raw_st

                    pv = int(part.vertex_barcode) if part.vertex_barcode else 0
                    evv = int(part.end_vertex_barcode) if part.end_vertex_barcode else 0

                    f.write(
                        "P {bc} {pid} {st} {px:.17g} {py:.17g} {pz:.17g} {e:.17g} {m:.17g} {pv} {ev}\n".format(
                            bc=bc,
                            pid=part.pdg_id,
                            st=out_status,
                            px=part.px,
                            py=part.py,
                            pz=part.pz,
                            e=part.energy,
                            m=part.mass,
                            pv=pv,
                            ev=evv,
                        )
                    )
