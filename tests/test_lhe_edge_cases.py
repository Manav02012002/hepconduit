from __future__ import annotations

from pathlib import Path

from hepconduit.io.lhe import iter_lhe


def test_lhe_edge_cases_fixture_parses_and_captures_weight_blocks_and_trailer() -> None:
    p = Path("tests/fixtures/edge_cases.lhe")
    evs = list(iter_lhe(str(p)))
    assert len(evs) == 1
    ev = evs[0]

    assert ev.n_particles in (0, 2)
    assert "lhe" in ev.extra

    rwgt = ev.extra["lhe"].get("rwgt", {})
    assert "mur=0.5_muf=0.5" in rwgt
    assert "mur=2.0_muf=2.0" in rwgt

    wblk = ev.extra["lhe"].get("weights", {})
    assert "nominal" in wblk
    assert "pdf_alt" in wblk

    tail = ev.extra["lhe"].get("tail", [])
    assert any("some_generator_token" in t for t in tail)

    # weights list includes: nominal header weight + block weights + rwgt
    assert len(ev.weights) >= 1 + len(rwgt) + len(wblk)
