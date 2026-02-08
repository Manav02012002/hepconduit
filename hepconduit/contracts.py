from __future__ import annotations
from .contract_packs import PACKS as EXTRA_PACKS, EXTRA_CONTRACTS

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import hepconduit

from .convert import convert, read
from .fingerprint import FingerprintConfig, fingerprint_event
from .validation import validate


@dataclass
class ContractResult:
    contract: str
    ok: bool
    details: Dict[str, Any]


@dataclass
class ContractPackResult:
    pack: str
    ok: bool
    results: List[ContractResult]


_BUILTIN_PACKS: Dict[str, List[str]] = {
    # Generator-level baseline invariants (format-agnostic)
    "generator_level_v1": ["validate_only_v1", "roundtrip_v1"],
    # HepMC3 fidelity: asserts graph/weights/units survive HepMC3 -> HepMC3
    "hepmc3_fidelity_v1": ["hepmc3_roundtrip_fidelity_v1"],
}

# Extend builtin packs with extra packs (e.g. Parquet/HepMC3 fidelity)
_BUILTIN_PACKS.update(EXTRA_PACKS)



def available_contracts() -> List[str]:
    # Builtin contracts implemented in this module, plus plugin/extra contracts.
    builtins = ["roundtrip_v1", "validate_only_v1", "hepmc3_roundtrip_fidelity_v1"]
    return sorted(set(builtins) | set(EXTRA_CONTRACTS.keys()))


def available_packs() -> List[str]:
    return sorted(_BUILTIN_PACKS.keys())


def run_contract(
    input_path: str,
    *,
    contract: str = "roundtrip_v1",
    to_format: str = "hepmc3",
    strict: bool = False,
) -> ContractResult:
    # Extra/plugin contracts dispatch
    if contract in EXTRA_CONTRACTS:
        r = EXTRA_CONTRACTS[contract](input_path)
        # Accept either this module's ContractResult or external (name/passed/message)
        if isinstance(r, ContractResult):
            return r
        if hasattr(r, 'name') and hasattr(r, 'passed'):
            return ContractResult(contract=r.name, ok=bool(r.passed), details={'message': getattr(r, 'message', '')})
        return ContractResult(contract=contract, ok=bool(getattr(r, 'passed', False)), details={'result': str(r)})

    if contract not in set(available_contracts()):
        raise ValueError(f"Unknown contract: {contract}. Available: {', '.join(available_contracts())}")

    mom_tol = 1e-6 if strict else 1e-4
    mass_tol = 1e-4 if strict else 1e-2

    if contract == "validate_only_v1":
        ef = read(input_path)
        rep = validate(ef, momentum_tolerance=mom_tol, mass_tolerance=mass_tol)
        ok = rep.is_valid
        return ContractResult(
            contract=contract,
            ok=ok,
            details={"validation": rep.to_dict() if hasattr(rep, "to_dict") else str(rep)},
        )

    if contract == "hepmc3_roundtrip_fidelity_v1":
        ef_in = read(input_path, format="hepmc3")
        cfg = FingerprintConfig(include_graph=True, include_weights=True)
        fp_in = [fingerprint_event(ev, cfg=cfg) for ev in ef_in.events]
        run_in = ef_in.run_info

        with tempfile.TemporaryDirectory(prefix="hepconduit_contract_") as td:
            td_path = Path(td)
            outp = td_path / "out.hepmc"
            convert(input_path, outp, input_format="hepmc3", output_format="hepmc3", quiet=True, report="none", provenance="none")
            ef_out = read(outp, format="hepmc3")

        run_out = ef_out.run_info
        fp_out = [fingerprint_event(ev, cfg=cfg) for ev in ef_out.events]

        ok = True
        reasons = []

        # Run-level: units + weight names
        if (run_in.extra or {}).get("units") != (run_out.extra or {}).get("units"):
            ok = False
            reasons.append("units_changed")
        if list(run_in.weight_names or []) != list(run_out.weight_names or []):
            ok = False
            reasons.append("weight_names_changed")

        # Event-level: counts, weights, graph stable fingerprints
        if len(ef_in.events) != len(ef_out.events):
            ok = False
            reasons.append("event_count_changed")
        if fp_in != fp_out:
            ok = False
            reasons.append("event_fingerprints_changed")

        return ContractResult(
            contract=contract,
            ok=ok,
            details={
                "tool": {"name": "hepconduit", "version": hepconduit.__version__},
                "reasons": reasons,
                "n_events": {"input": len(ef_in.events), "out": len(ef_out.events)},
                "run": {"input": {"units": (run_in.extra or {}).get("units"), "weight_names": run_in.weight_names},
                        "out": {"units": (run_out.extra or {}).get("units"), "weight_names": run_out.weight_names}},
            },
        )

    # roundtrip_v1: parse -> validate -> convert -> reparse -> invariants
    ef_in = read(input_path)
    rep_in = validate(ef_in, momentum_tolerance=mom_tol, mass_tolerance=mass_tol)
    cfg = FingerprintConfig()
    fp_in = [fingerprint_event(ev, cfg=cfg) for ev in ef_in.events]

    with tempfile.TemporaryDirectory(prefix="hepconduit_contract_") as td:
        td_path = Path(td)
        mid = td_path / f"mid.{to_format}"
        back = td_path / "back.lhe"

        convert(input_path, mid, output_format=to_format, quiet=True, report="none", provenance="none")
        convert(mid, back, output_format="lhe", quiet=True, report="none", provenance="none")

        ef_back = read(back)
        rep_back = validate(ef_back, momentum_tolerance=mom_tol, mass_tolerance=mass_tol)
        fp_back = [fingerprint_event(ev, cfg=cfg) for ev in ef_back.events]

    ok = True
    reasons = []
    if not rep_in.is_valid:
        ok = False
        reasons.append("input_failed_validation")
    if not rep_back.is_valid:
        ok = False
        reasons.append("roundtrip_failed_validation")
    if len(ef_in.events) != len(ef_back.events):
        ok = False
        reasons.append("event_count_changed")
    if fp_in != fp_back:
        ok = False
        reasons.append("fingerprints_changed")

    return ContractResult(
        contract=contract,
        ok=ok,
        details={
            "tool": {"name": "hepconduit", "version": hepconduit.__version__},
            "to_format": to_format,
            "reasons": reasons,
            "n_events": {"input": len(ef_in.events), "back": len(ef_back.events)},
            "validation": {
                "input": rep_in.to_dict() if hasattr(rep_in, "to_dict") else str(rep_in),
                "back": rep_back.to_dict() if hasattr(rep_back, "to_dict") else str(rep_back),
            },
        },
    )


def run_contract_pack(
    input_path: str,
    *,
    pack: str,
    to_format: str = "hepmc3",
    strict: bool = False,
) -> ContractPackResult:
    if pack not in _BUILTIN_PACKS:
        raise ValueError(f"Unknown pack: {pack}. Available: {', '.join(available_packs())}")

    results: List[ContractResult] = []
    ok = True
    for c in _BUILTIN_PACKS[pack]:
        # hepmc3 fidelity contract doesn't use to_format; others do.
        if c == "hepmc3_roundtrip_fidelity_v1":
            r = run_contract(input_path, contract=c, strict=strict)
        else:
            r = run_contract(input_path, contract=c, to_format=to_format, strict=strict)
        results.append(r)
        if not r.ok:
            ok = False

    return ContractPackResult(pack=pack, ok=ok, results=results)


@dataclass
class CertifyReport:
    ok: bool
    pack: Optional[str] = None
    contract: Optional[str] = None
    to_format: str = "hepmc3"
    results: List[ContractResult] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": bool(self.ok),
            "pack": self.pack,
            "contract": self.contract,
            "to_format": self.to_format,
            "results": [{"contract": r.contract, "ok": r.ok, "details": r.details} for r in (self.results or [])],
        }

    def __str__(self) -> str:
        if self.pack:
            header = f"Contract pack {self.pack}: {'PASS' if self.ok else 'FAIL'}"
        else:
            header = f"Contract {self.contract}: {'PASS' if self.ok else 'FAIL'}"
        lines = [header]
        for r in (self.results or []):
            lines.append(f"  - {r.contract}: {'PASS' if r.ok else 'FAIL'}")
            if not r.ok:
                reasons = (r.details or {}).get("reasons")
                if reasons:
                    lines.append(f"      reasons: {reasons}")
        return "\n".join(lines)


def certify(input_path: str, *, contract: str = "roundtrip_v1", to_format: str = "hepmc3", strict: bool = False) -> CertifyReport:
    r = run_contract(input_path, contract=contract, to_format=to_format, strict=strict)
    return CertifyReport(ok=r.ok, contract=contract, to_format=to_format, results=[r])


def certify_pack(input_path: str, *, pack: str, to_format: str = "hepmc3", strict: bool = False) -> CertifyReport:
    pr = run_contract_pack(input_path, pack=pack, to_format=to_format, strict=strict)
    return CertifyReport(ok=pr.ok, pack=pack, to_format=to_format, results=pr.results)

