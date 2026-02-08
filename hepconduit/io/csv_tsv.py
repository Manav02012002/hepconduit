from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ..models import Event, EventFile, Particle, RunInfo
from .reader_base import Reader
from .writer_base import Writer

DEFAULT_FIELDS = [
    "event_number",
    "pdg_id",
    "status",
    "mother1",
    "mother2",
    "color1",
    "color2",
    "px",
    "py",
    "pz",
    "energy",
    "mass",
    "spin",
    "barcode",
    "vertex_barcode",
    "end_vertex_barcode",
]


def _delimiter_from_fmt(fmt: str) -> str:
    return "\t" if fmt == "tsv" else ","


class CSVReader(Reader):
    def __init__(self, delimiter: str = ","):
        self.delimiter = delimiter

    def iter_events(self, path: str) -> Iterator[Event]:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            current_evt = None
            particles: list[Particle] = []
            for row in reader:
                evt_no = int(row.get("event_number", "0") or "0")
                if current_evt is None:
                    current_evt = evt_no
                if evt_no != current_evt:
                    yield Event(event_number=current_evt, particles=particles)
                    current_evt = evt_no
                    particles = []
                p = Particle(
                    pdg_id=int(row["pdg_id"]),
                    status=int(row["status"]),
                    mother1=int(row.get("mother1", "0") or "0"),
                    mother2=int(row.get("mother2", "0") or "0"),
                    color1=int(row.get("color1", "0") or "0"),
                    color2=int(row.get("color2", "0") or "0"),
                    px=float(row["px"]),
                    py=float(row["py"]),
                    pz=float(row["pz"]),
                    energy=float(row.get("energy", row.get("E", "0")) or "0"),
                    mass=float(row.get("mass", row.get("m", "0")) or "0"),
                    spin=float(row.get("spin", "9") or "9"),
                    barcode=int(row.get("barcode", "0") or "0"),
                    vertex_barcode=int(row.get("vertex_barcode", "0") or "0"),
                    end_vertex_barcode=int(row.get("end_vertex_barcode", "0") or "0"),
                )
                particles.append(p)
            if current_evt is not None:
                yield Event(event_number=current_evt, particles=particles)

    def read(self, path: str) -> EventFile:
        events = list(self.iter_events(path))
        return EventFile(run_info=RunInfo(), events=events, format_name="csv")


class CSVWriter(Writer):
    def __init__(self, delimiter: str = ","):
        self.delimiter = delimiter

    def write(self, path: str, events: Iterable[Event], run_info: Optional[RunInfo], **kwargs) -> None:
        fields = kwargs.get("fields", DEFAULT_FIELDS)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, delimiter=self.delimiter)
            writer.writeheader()
            for ev in events:
                for p in ev.particles:
                    row = {
                        "event_number": ev.event_number,
                        **p.to_dict(),
                    }
                    # to_dict uses 'energy' but field list uses 'energy'
                    writer.writerow({k: row.get(k, "") for k in fields})
