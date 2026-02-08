from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .convert import read
from .fingerprint import FingerprintConfig, fingerprint_event


def diff_files(
    path_a: str,
    path_b: str,
    *,
    format_a: Optional[str] = None,
    format_b: Optional[str] = None,
    by: str = "fingerprint",
    abs_tol: float = 1e-6,
) -> Dict[str, Any]:
    """Compute a semantic diff summary between two event files."""

    ef_a = read(path_a, format=format_a)
    ef_b = read(path_b, format=format_b)

    if by not in {"fingerprint", "index"}:
        raise ValueError("--by must be 'fingerprint' or 'index'")

    cfg = FingerprintConfig(abs_tol=abs_tol)

    if by == "fingerprint":
        from collections import Counter

        ca = Counter(fingerprint_event(ev, cfg=cfg) for ev in ef_a.events)
        cb = Counter(fingerprint_event(ev, cfg=cfg) for ev in ef_b.events)
        keys = set(ca) | set(cb)
        common = sum(min(ca.get(k, 0), cb.get(k, 0)) for k in keys)
        added = sum(max(0, cb.get(k, 0) - ca.get(k, 0)) for k in keys)
        removed = sum(max(0, ca.get(k, 0) - cb.get(k, 0)) for k in keys)

        example_added = [k for k in cb if cb[k] > ca.get(k, 0)][:5]
        example_removed = [k for k in ca if ca[k] > cb.get(k, 0)][:5]

        return {
            "mode": "fingerprint",
            "n_a": len(ef_a.events),
            "n_b": len(ef_b.events),
            "common": common,
            "added": added,
            "removed": removed,
            "example_added": example_added,
            "example_removed": example_removed,
        }

    # by index: compute drift stats
    n = min(len(ef_a.events), len(ef_b.events))
    weight_diffs = []
    max_dp = 0.0
    mean_dp_sum = 0.0
    n_part_comp = 0
    for i in range(n):
        ea = ef_a.events[i]
        eb = ef_b.events[i]
        weight_diffs.append(float(eb.weight) - float(ea.weight))
        # Compare final-state particles by sorted (pdg,px,py,pz,e) quantized; report drift
        fa = sorted([(p.pdg_id, p.px, p.py, p.pz, p.energy) for p in ea.particles if p.is_final])
        fb = sorted([(p.pdg_id, p.px, p.py, p.pz, p.energy) for p in eb.particles if p.is_final])
        m = min(len(fa), len(fb))
        for j in range(m):
            _, ax, ay, az, ae = fa[j]
            _, bx, by_, bz, be = fb[j]
            dp = abs(bx-ax) + abs(by_-ay) + abs(bz-az) + abs(be-ae)
            if dp > max_dp:
                max_dp = dp
            mean_dp_sum += dp
            n_part_comp += 1

    mean_dp = mean_dp_sum / max(1, n_part_comp)
    mean_dw = sum(weight_diffs) / max(1, len(weight_diffs))
    max_dw = max((abs(x) for x in weight_diffs), default=0.0)
    return {
        "mode": "index",
        "n_a": len(ef_a.events),
        "n_b": len(ef_b.events),
        "compared_events": n,
        "weight": {"mean_delta": mean_dw, "max_abs_delta": max_dw},
        "final_state_drift": {"mean_L1": mean_dp, "max_L1": max_dp},
    }
