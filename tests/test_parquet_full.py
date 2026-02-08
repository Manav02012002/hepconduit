import importlib
import pytest

from hepconduit.convert import read, write
from hepconduit.fingerprint import FingerprintConfig, fingerprint_event

try:
    importlib.import_module("pyarrow")
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False

pytestmark = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed; parquet integration tests skipped")


def _fingerprints(evfile, *, include_graph=False, include_weights=False):
    cfg = FingerprintConfig(include_graph=include_graph, include_weights=include_weights)
    return [fingerprint_event(ev, cfg=cfg) for ev in evfile.events]


def test_parquet_columnar_roundtrip(tmp_path):
    inp = "tests/fixtures/pp_to_wpenu.lhe"
    ef = read(inp, format="lhe")

    outp = tmp_path / "out_columnar.parquet"
    write(outp, ef, format="parquet", columnar=True, metadata={"hepconduit_schema": "hepconduit.event.v1.columnar"})

    ef2 = read(outp, format="parquet")
    assert len(ef2.events) == len(ef.events)
    assert _fingerprints(ef, include_graph=False, include_weights=False) == _fingerprints(ef2, include_graph=False, include_weights=False)


def test_parquet_flat_roundtrip(tmp_path):
    inp = "tests/fixtures/pp_to_wpenu.lhe"
    ef = read(inp, format="lhe")

    outp = tmp_path / "out_flat.parquet"
    write(outp, ef, format="parquet", columnar=False, metadata={"hepconduit_schema": "hepconduit.event.v1.flat"})

    ef2 = read(outp, format="parquet")
    assert len(ef2.events) == len(ef.events)
    assert _fingerprints(ef, include_graph=False, include_weights=False) == _fingerprints(ef2, include_graph=False, include_weights=False)
