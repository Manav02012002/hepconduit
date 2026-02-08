from __future__ import annotations

import gzip
import io
import re
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ..models import Event, EventFile, Particle, ProcessInfo, RunInfo
from .reader_base import Reader
from .writer_base import Writer

_TAG_EVENT_OPEN = re.compile(r"<event\b")
_TAG_EVENT_CLOSE = re.compile(r"</event>")
_TAG_INIT_OPEN = re.compile(r"<init\b")
_TAG_INIT_CLOSE = re.compile(r"</init>")
_TAG_GENERATOR = re.compile(r"<generator\b[^>]*>(.*?)</generator>", re.IGNORECASE|re.DOTALL)


def _open_text(path: str):
    p = Path(path)
    if p.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(p, "rb"), encoding="utf-8", errors="replace")
    return open(p, "r", encoding="utf-8", errors="replace")


def _parse_init(lines: list[str]) -> RunInfo:
    # First non-empty line contains beam IDs and energies
    beam_pdg = (0, 0)
    beam_e = (0.0, 0.0)
    processes: list[ProcessInfo] = []
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        parts = s.split()
        if len(parts) >= 4:
            try:
                beam_pdg = (int(parts[0]), int(parts[1]))
                beam_e = (float(parts[2]), float(parts[3]))
            except Exception:
                pass
            break
    # Process lines often follow; try parse as LPRUP lines: XSECUP XERRUP XMAXUP LPRUP
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        parts = s.split()
        if len(parts) == 4:
            try:
                xsec, xerr, xmax = float(parts[0]), float(parts[1]), float(parts[2])
                lprup = int(parts[3])
                # heuristically: ignore the first beam line if it had ints+floats
                if lprup not in (beam_pdg[0], beam_pdg[1]):
                    processes.append(ProcessInfo(process_id=lprup, cross_section=xsec, cross_section_error=xerr, max_weight=xmax))
            except Exception:
                continue
    return RunInfo(beam_pdg_id=beam_pdg, beam_energy=beam_e, processes=processes)


def iter_lhe(path: str) -> Iterator[Event]:
    with _open_text(path) as f:
        in_event = False
        buf: list[str] = []
        event_no = 0
        for line in f:
            if not in_event:
                if _TAG_EVENT_OPEN.search(line):
                    in_event = True
                    buf = []
                continue
            else:
                if _TAG_EVENT_CLOSE.search(line):
                    # parse buffered event
                    event_no += 1
                    ev = _parse_event_block(buf, event_no)
                    yield ev
                    in_event = False
                    buf = []
                else:
                    buf.append(line)


def _parse_event_block(lines: list[str], event_number: int) -> Event:
    # first non-empty non-comment line is header
    header = None
    idx = 0
    while idx < len(lines):
        s = lines[idx].strip()
        idx += 1
        if not s or s.startswith("#"):
            continue
        header = s
        break
    if header is None:
        return Event(event_number=event_number)

    hp = header.split()
    # nup idprup xwgtup scalup aqedup aqcdup
    nup = int(hp[0])
    process_id = int(hp[1]) if len(hp) > 1 else 0
    weight = float(hp[2]) if len(hp) > 2 else 1.0
    scale = float(hp[3]) if len(hp) > 3 else 0.0
    aqed = float(hp[4]) if len(hp) > 4 else 0.0
    aqcd = float(hp[5]) if len(hp) > 5 else 0.0

    particles: list[Particle] = []
    # next nup lines are particles
    for i in range(nup):
        if idx >= len(lines):
            break
        s = lines[idx].strip()
        idx += 1
        if not s or s.startswith("#"):
            i -= 1
            continue
        cols = s.split()
        # id status mother1 mother2 col1 col2 px py pz E M lifetime spin
        pdg_id = int(cols[0])
        status = int(cols[1])
        mother1 = int(cols[2])
        mother2 = int(cols[3])
        c1 = int(cols[4])
        c2 = int(cols[5])
        px = float(cols[6]); py = float(cols[7]); pz = float(cols[8])
        e = float(cols[9]); m = float(cols[10])
        spin = float(cols[12]) if len(cols) > 12 else 9.0
        particles.append(Particle(
            pdg_id=pdg_id,
            status=status,
            mother1=mother1,
            mother2=mother2,
            color1=c1,
            color2=c2,
            px=px,
            py=py,
            pz=pz,
            energy=e,
            mass=m,
            spin=spin,
        ))

    return Event(
        event_number=event_number,
        particles=particles,
        process_id=process_id,
        scale=scale,
        alpha_qed=aqed,
        alpha_qcd=aqcd,
        weights=[weight],
        n_particles=nup,
    )


class LHEReader(Reader):
    def iter_events(self, path: str) -> Iterator[Event]:
        return iter_lhe(path)

    def read(self, path: str) -> EventFile:
        run = self.read_run_info(path)
        events = list(iter_lhe(path))
        return EventFile(run_info=run, events=events, format_name="lhe")

    def read_run_info(self, path: str) -> RunInfo:
        generator_name = ""
        generator_version = ""
        init_lines: list[str] = []
        in_init = False
        with _open_text(path) as f:
            # Try to sniff generator tag quickly
            head = ""
            for _ in range(200):
                ln = f.readline()
                if not ln:
                    break
                head += ln
                if "</generator>" in ln.lower():
                    break
            m = _TAG_GENERATOR.search(head)
            if m:
                gen = re.sub(r"\s+", " ", m.group(1).strip())
                # common pattern: "MadGraph5_aMC@NLO v2.9.18"
                if " v" in gen:
                    generator_name, generator_version = gen.split(" v", 1)
                else:
                    generator_name = gen

        with _open_text(path) as f2:
            for line in f2:
                if not in_init:
                    if _TAG_INIT_OPEN.search(line):
                        in_init = True
                        init_lines = []
                    continue
                else:
                    if _TAG_INIT_CLOSE.search(line):
                        break
                    init_lines.append(line)

        run = _parse_init(init_lines)
        run.generator_name = generator_name
        run.generator_version = generator_version
        return run


class LHEWriter(Writer):
    def write(self, path: str, events: Iterable[Event], run_info: Optional[RunInfo], **kwargs) -> None:
        p = Path(path)
        if p.suffix == ".gz":
            fh = gzip.open(p, "wt", encoding="utf-8")
        else:
            fh = open(p, "w", encoding="utf-8")
        with fh as out:
            out.write("<LesHouchesEvents version=\"3.0\">\n")
            # init block
            run = run_info or RunInfo()
            out.write("<init>\n")
            out.write(f"{run.beam_pdg_id[0]} {run.beam_pdg_id[1]} {run.beam_energy[0]:.8g} {run.beam_energy[1]:.8g} 0 0 0 0 0 0\n")
            for proc in run.processes:
                out.write(f"{proc.cross_section:.8g} {proc.cross_section_error:.8g} {proc.max_weight:.8g} {proc.process_id}\n")
            out.write("</init>\n")
            if run.generator_name:
                gen = run.generator_name
                if run.generator_version:
                    gen += f" v{run.generator_version}"
                out.write(f"<generator>{gen}</generator>\n")

            for ev in events:
                out.write("<event>\n")
                nup = len(ev.particles)
                w = ev.weight
                out.write(f"{nup} {ev.process_id} {w:.16g} {ev.scale:.16g} {ev.alpha_qed:.16g} {ev.alpha_qcd:.16g}\n")
                for i, p in enumerate(ev.particles, start=1):
                    # lifetime not modeled -> 0.0
                    out.write(
                        f"{p.pdg_id} {p.status} {p.mother1} {p.mother2} {p.color1} {p.color2} "
                        f"{p.px:.16g} {p.py:.16g} {p.pz:.16g} {p.energy:.16g} {p.mass:.16g} 0 {p.spin:.16g}\n"
                    )
                out.write("</event>\n")
            out.write("</LesHouchesEvents>\n")
