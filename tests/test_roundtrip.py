from pathlib import Path

import hepconduit


def test_roundtrip_lhe_hepmc3_lhe(tmp_path: Path):
    inp = Path(__file__).parent / "fixtures" / "pp_to_wpenu.lhe"
    hepmc = tmp_path / "out.hepmc"
    back = tmp_path / "back.lhe"

    r1 = hepconduit.convert(inp, hepmc, validate=True, quiet=True)
    assert r1["n_output"] == 3

    ef_hepmc = hepconduit.read(hepmc)
    rep_hepmc = hepconduit.validation.validate(ef_hepmc)
    assert rep_hepmc.is_valid, str(rep_hepmc)

    r2 = hepconduit.convert(hepmc, back, validate=True, quiet=True)
    assert r2["n_output"] == 3

    ef_back = hepconduit.read(back)
    rep_back = hepconduit.validation.validate(ef_back)
    assert rep_back.is_valid, str(rep_back)

    # Basic invariants
    ef_in = hepconduit.read(inp)
    assert len(ef_in.events) == 3
    assert len(ef_hepmc.events) == 3
    assert len(ef_back.events) == 3
