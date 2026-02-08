from __future__ import annotations

from typing import Any, Dict, List


def doctor_report() -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    # Core import
    try:
        import hepconduit  # noqa: F401
        checks.append({"name": "hepconduit import", "ok": True, "detail": "import ok"})
    except Exception as e:
        checks.append({"name": "hepconduit import", "ok": False, "detail": str(e)})

    # Optional deps
    try:
        import pyarrow  # noqa: F401
        checks.append({"name": "pyarrow (parquet)", "ok": True, "detail": "installed"})
    except Exception:
        checks.append({"name": "pyarrow (parquet)", "ok": True, "detail": "not installed (optional)"})

    try:
        import particle  # noqa: F401
        checks.append({"name": "particle (pdg)", "ok": True, "detail": "installed"})
    except Exception:
        checks.append({"name": "particle (pdg)", "ok": True, "detail": "not installed (optional)"})

    ok_all = all(c["ok"] for c in checks)
    summary = "hepconduit doctor: OK" if ok_all else "hepconduit doctor: FAIL"

    return {"summary": summary, "checks": checks}
