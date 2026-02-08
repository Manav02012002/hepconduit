"""PDG helpers.

- If scikit-hep "particle" is installed, use it for names and validity.
- Otherwise, fall back to a small built-in map for common particles.
"""

from __future__ import annotations

from typing import Optional

_FALLBACK_NAMES = {
    1: "d",
    2: "u",
    3: "s",
    4: "c",
    5: "b",
    6: "t",
    11: "e-",
    -11: "e+",
    13: "mu-",
    -13: "mu+",
    15: "tau-",
    -15: "tau+",
    12: "nu_e",
    -12: "nu_ebar",
    14: "nu_mu",
    -14: "nu_mubar",
    16: "nu_tau",
    -16: "nu_taubar",
    21: "g",
    22: "gamma",
    23: "Z0",
    24: "W+",
    -24: "W-",
    25: "H",
    2212: "p",
    -2212: "pbar",
}

try:
    from particle import PDGID as _PDGID  # type: ignore
    from particle import Particle as _Particle  # type: ignore
except Exception:  # pragma: no cover
    _PDGID = None
    _Particle = None


def is_valid_pdg_id(pdg_id: int) -> bool:
    if _PDGID is None:
        return True
    try:
        return bool(_PDGID(pdg_id).is_valid)
    except Exception:
        return False


def name(pdg_id: int) -> str:
    if _Particle is not None:
        try:
            return _Particle.from_pdgid(pdg_id).name
        except Exception:
            pass
    return _FALLBACK_NAMES.get(pdg_id, str(pdg_id))


def mass_geV(pdg_id: int) -> Optional[float]:
    if _Particle is None:
        return None
    try:
        p = _Particle.from_pdgid(pdg_id)
        if p.mass is None:
            return None
        return float(p.mass)
    except Exception:
        return None

# Backwards-compatible alias
is_valid = is_valid_pdg_id

