from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .reader_base import Reader
from .writer_base import Writer


@dataclass(frozen=True)
class FormatHandlers:
    reader: Callable[[], Reader]
    writer: Callable[[], Writer]


_REGISTRY: dict[str, FormatHandlers] = {}


def register(fmt: str, reader: Callable[[], Reader], writer: Callable[[], Writer]) -> None:
    _REGISTRY[fmt] = FormatHandlers(reader=reader, writer=writer)


def detect_format(filepath: str | Path) -> str:
    p = Path(filepath)
    suffixes = list(p.suffixes)
    if suffixes and suffixes[-1] == ".gz":
        suffixes = suffixes[:-1]
    if not suffixes:
        raise ValueError(f"Cannot detect format from filename: {p}")
    ext = suffixes[-1].lower()
    ext_map = {
        ".lhe": "lhe",
        ".hepmc": "hepmc3",
        ".hepmc3": "hepmc3",
        ".csv": "csv",
        ".tsv": "tsv",
        ".tab": "tsv",
        ".parquet": "parquet",
        ".pq": "parquet",
    }
    fmt = ext_map.get(ext)
    if fmt is None:
        raise ValueError(f"Unknown file extension '{ext}' in {p}")
    return fmt


def get_reader(fmt: str) -> Reader:
    if fmt not in _REGISTRY:
        raise ValueError(f"No reader registered for format: {fmt}")
    return _REGISTRY[fmt].reader()


def get_writer(fmt: str) -> Writer:
    if fmt not in _REGISTRY:
        raise ValueError(f"No writer registered for format: {fmt}")
    return _REGISTRY[fmt].writer()
