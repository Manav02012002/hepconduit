from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _git_sha(repo_root: str | Path | None = None) -> str:
    """Best-effort git SHA for provenance.

    Returns empty string if not in a git worktree or git is unavailable.
    """
    try:
        cwd = str(repo_root) if repo_root is not None else None
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cwd, stderr=subprocess.DEVNULL)
        return out.decode("utf-8").strip()
    except Exception:
        return ""


def _repo_root_from_here() -> Optional[Path]:
    # Walk upwards from this file; if a .git exists, treat that as root.
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / ".git").exists():
            return parent
    return None


def build_provenance(
    *,
    tool: str,
    tool_version: str,
    input_path: str | Path,
    output_path: str | Path,
    input_format: str,
    output_format: str,
    argv: list[str],
    contract_id: str = "",
    loss_hash: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    repo_root = _repo_root_from_here()
    prov: Dict[str, Any] = {
        "tool": tool,
        "tool_version": tool_version,
        "git_sha": _git_sha(repo_root),
        "utc_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "input": {
            "path": str(input_path),
            "sha256": _sha256_file(input_path),
            "format": input_format,
        },
        "output": {
            "path": str(output_path),
            "format": output_format,
        },
        "argv": argv,
        "contract_id": contract_id,
        "loss_hash": loss_hash,
    }
    if extra:
        prov["extra"] = extra
    return prov


def stable_json_dumps(obj: Any) -> str:
    """Deterministic JSON for hashing / embedding."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
