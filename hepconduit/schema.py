from __future__ import annotations

from typing import Any, Dict, List

from .convert import read, write


KNOWN_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "hepconduit.event.v1.flat": {
        "format": "parquet",
        "layout": "flat",
        "description": "particle-per-row flat table."
    },
    "hepconduit.event.v1.columnar": {
        "format": "parquet",
        "layout": "columnar",
        "description": "event-per-row with particles list-of-struct."
    },
}


def list_schemas() -> List[Dict[str, Any]]:
    out = []
    for name, meta in sorted(KNOWN_SCHEMAS.items()):
        out.append({"name": name, **meta})
    return out


def upgrade_parquet(input_path: str, output_path: str, *, to_schema: str) -> None:
    if to_schema not in KNOWN_SCHEMAS:
        raise ValueError(f"Unknown schema: {to_schema}")
    spec = KNOWN_SCHEMAS[to_schema]
    if spec["format"] != "parquet":
        raise ValueError("upgrade_parquet only supports parquet targets")

    ef = read(input_path, format="parquet")
    columnar = spec["layout"] == "columnar"
    # Writer will embed schema name in metadata
    write(output_path, ef, format="parquet", columnar=columnar, metadata={"hepconduit_schema": to_schema})
