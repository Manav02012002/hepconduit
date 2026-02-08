from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .convert import read, write
from .fingerprint import FingerprintConfig, fingerprint_event


@dataclass(frozen=True)
class ContractResult:
    name: str
    passed: bool
    message: str = ""


def _validate(path: str) -> tuple[int, int]:
    """Return (n_errors, n_warnings) using hepconduit's internal validator."""
    try:
        from .convert import read
        from .validation import validate
        ef = read(path)
        rep = validate(ef)
        # ValidationReport exposes n_errors/n_warnings
        nerr = int(getattr(rep, "n_errors", 0))
        nwarn = int(getattr(rep, "n_warnings", 0))
        return nerr, nwarn
    except Exception:
        # As a last resort, consider any exception a validation error
        return 1, 0


def _strict_fps(path: str, fmt: Optional[str] = None) -> List[str]:
    ef = read(path, format=fmt) if fmt else read(path)
    cfg = FingerprintConfig(include_graph=True, include_weights=True)
    return [fingerprint_event(ev, cfg=cfg) for ev in ef.events]


def hepmc3_roundtrip_fidelity_v1(path: str) -> ContractResult:
    """
    HepMC3 -> HepMC3 should preserve strict graph+weights fingerprint.
    """
    import tempfile
    from pathlib import Path

    errs, warns = _validate(path)
    if errs:
        return ContractResult("hepmc3_roundtrip_fidelity_v1", False, f"input invalid: {errs} errors")

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        mid = td / "mid.hepmc"
        write(mid, read(path, format="hepmc3"), format="hepmc3")
        errs2, warns2 = _validate(str(mid))
        if errs2:
            return ContractResult("hepmc3_roundtrip_fidelity_v1", False, f"roundtrip invalid: {errs2} errors")

        a = _strict_fps(path, fmt="hepmc3")
        b = _strict_fps(str(mid), fmt="hepmc3")
        if a != b:
            return ContractResult("hepmc3_roundtrip_fidelity_v1", False, "strict fingerprints differ after HepMC3->HepMC3")

    return ContractResult("hepmc3_roundtrip_fidelity_v1", True, "ok")


def parquet_fidelity_v1(path: str) -> ContractResult:
    """
    Any input -> Parquet -> Parquet-read must preserve strict (graph+weights) fingerprints.
    """
    import tempfile
    from pathlib import Path

    errs, warns = _validate(path)
    if errs:
        return ContractResult("parquet_fidelity_v1", False, f"input invalid: {errs} errors")

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        pq = td / "out.parquet"

        ef = read(path)
        write(pq, ef, format="parquet", columnar=True, metadata={"hepconduit_schema": "hepconduit.event.v1.columnar"})
        errs_pq, _ = _validate(str(pq))
        if errs_pq:
            return ContractResult("parquet_fidelity_v1", False, f"parquet invalid: {errs_pq} errors")

        ef2 = read(str(pq), format="parquet")

        cfg = FingerprintConfig(include_graph=True, include_weights=True)
        a = [fingerprint_event(ev, cfg=cfg) for ev in ef.events]
        b = [fingerprint_event(ev, cfg=cfg) for ev in ef2.events]
        if a != b:
            return ContractResult("parquet_fidelity_v1", False, "strict fingerprints differ after ->Parquet")

    return ContractResult("parquet_fidelity_v1", True, "ok")


# Extra contracts registry: name -> callable(path)->ContractResult
EXTRA_CONTRACTS: Dict[str, Callable[[str], ContractResult]] = {
    "hepmc3_roundtrip_fidelity_v1": hepmc3_roundtrip_fidelity_v1,
    "parquet_fidelity_v1": parquet_fidelity_v1,
}

# Packs are lists of contract *names* (strings), matching contracts.py expectations.
PACKS: Dict[str, List[str]] = {
    # we do NOT redefine generator_level_v1 here; builtin owns it.
    "hepmc3_fidelity_v1": ["hepmc3_roundtrip_fidelity_v1"],
    "parquet_fidelity_v1": ["parquet_fidelity_v1"],
}
