from __future__ import annotations

from pathlib import Path

from hepconduit.io.hepmc3 import iter_hepmc3_with_runinfo


def test_hepmc3_edge_cases_preserves_event_attributes_and_parses_run_info() -> None:
    p = Path("tests/fixtures/edge_cases.hepmc")
    run, it = iter_hepmc3_with_runinfo(str(p))
    evs = list(it)
    assert len(evs) == 1
    ev = evs[0]

    assert ev.event_number == 7
    assert ev.extra.get("hepmc3", {}).get("A") == ["A key1 value1"]
    assert len(ev.weights) >= 1

    # Run-level F/C records should be preserved and parsed
    h3 = run.extra.get("hepmc3", {})
    assert "F" in h3 and isinstance(h3["F"], list) and h3["F"]
    assert "C" in h3 and isinstance(h3["C"], list) and h3["C"]
    assert "F_parsed" in h3 and h3["F_parsed"]
    assert "C_parsed" in h3 and h3["C_parsed"]

    # Convenience alias
    cs = run.extra.get("cross_section", {})
    assert "xsec" in cs

    # Event-level C record should also be captured
    e3 = ev.extra.get("hepmc3", {})
    assert "C" in e3 and e3["C"]
    assert "C_parsed" in e3 and e3["C_parsed"]
