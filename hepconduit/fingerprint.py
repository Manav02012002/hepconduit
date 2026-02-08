from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Tuple

from .models import Event, Particle


def _quantize(x: float, *, abs_tol: float) -> int:
    """Quantize a float into an int bucket.

    abs_tol should be chosen above typical formatting/IO roundoff (e.g. 1e-6).
    """
    if abs_tol <= 0:
        raise ValueError("abs_tol must be > 0")
    return int(round(x / abs_tol))


def _particle_graph_key(p: Particle) -> Tuple[int, int, int]:
    return (int(getattr(p,'barcode',0) or 0), int(getattr(p,'vertex_barcode',0) or 0), int(getattr(p,'end_vertex_barcode',0) or 0))


def _particle_key(p: Particle, *, abs_tol: float) -> Tuple[int, int, int, int, int, int]:
    # Only fields intended to be format-stable.
    return (
        int(p.status),
        int(p.pdg_id),
        _quantize(p.px, abs_tol=abs_tol),
        _quantize(p.py, abs_tol=abs_tol),
        _quantize(p.pz, abs_tol=abs_tol),
        _quantize(p.energy, abs_tol=abs_tol),
    )


@dataclass(frozen=True)
class FingerprintConfig:
    version: str = "event_fingerprint_v1"
    abs_tol: float = 1e-4
    include_intermediate: bool = True
    include_incoming: bool = True
    include_weights: bool = False
    include_graph: bool = False
    include_process_id: bool = False


def fingerprint_event(ev: Event, *, cfg: FingerprintConfig = FingerprintConfig()) -> str:
    """Stable event fingerprint across supported formats.

    Designed for:
      - deduplication
      - joins
      - semantic diff summaries

    Notes:
      - mother/vertex graph is excluded by default for cross-format stability.
        You can opt in via include_graph=True when you know inputs preserve it (e.g. HepMC3).
      - uses tolerance-aware quantization to be stable across IO formatting.
    """

    parts = []
    for p in ev.particles:
        if p.status == 3:
            continue
        if p.is_incoming and not cfg.include_incoming:
            continue
        if p.is_intermediate and not cfg.include_intermediate:
            continue
        parts.append(_particle_key(p, abs_tol=cfg.abs_tol))
    parts.sort()

    h = hashlib.sha256()
    h.update(cfg.version.encode("utf-8"))
    h.update(b"\0")
    if cfg.include_process_id:
        h.update(str(int(ev.process_id)).encode("utf-8"))
        h.update(b"\0")
    # particles
    for t in parts:
        h.update(",".join(str(x) for x in t).encode("utf-8"))
        h.update(b";")


    if cfg.include_graph:
        h.update(b"|g|")
        # stable multiset of (barcode, prod_vtx, end_vtx)
        gk = [_particle_graph_key(p) for p in ev.particles if p.status != 3]
        gk.sort()
        for t in gk:
            h.update(",".join(str(x) for x in t).encode("utf-8"))
            h.update(b";")

    if cfg.include_weights and ev.weights:
        # Quantize weights using same abs_tol for simplicity
        h.update(b"|w|")
        for w in ev.weights:
            h.update(str(_quantize(float(w), abs_tol=cfg.abs_tol)).encode("utf-8"))
            h.update(b",")

    return h.hexdigest()


def fingerprints(events: Iterable[Event], *, cfg: FingerprintConfig = FingerprintConfig()):
    for ev in events:
        yield fingerprint_event(ev, cfg=cfg)
