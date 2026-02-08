import subprocess
from pathlib import Path

import pytest

import hepconduit

FIXTURES = Path(__file__).parent / "fixtures"
LHE_FILE = FIXTURES / "pp_to_wpenu.lhe"
HEPMC_FILE = FIXTURES / "pp_to_wpenu.hepmc"


def test_read_lhe_basic():
    ef = hepconduit.read(LHE_FILE)
    assert ef.format_name == "lhe"
    assert len(ef.events) == 3

    event = ef.events[0]
    assert len(event.particles) == 5
    assert event.process_id == 1
    assert event.scale == pytest.approx(80.379, rel=1e-3)

    u = event.particles[0]
    assert u.pdg_id == 2
    assert u.status == -1
    assert u.pz == pytest.approx(40.1895, rel=1e-3)

    w = event.particles[2]
    assert w.pdg_id == 24
    assert w.status == 2
    assert w.mass == pytest.approx(80.379, rel=1e-3)

    assert ef.run_info.beam_pdg_id == (2212, 2212)
    assert ef.run_info.beam_energy == pytest.approx((6500.0, 6500.0), rel=1e-3)
    assert ef.run_info.generator_name == "MadGraph5_aMC@NLO"


def test_streaming_lhe_iter():
    from hepconduit.io.lhe import iter_lhe
    assert len(list(iter_lhe(str(LHE_FILE)))) == 3


def test_read_hepmc_basic():
    ef = hepconduit.read(HEPMC_FILE)
    assert ef.format_name == "hepmc3"
    assert len(ef.events) == 3
    ev0 = ef.events[0]
    assert len(ev0.particles) == 5


def test_filtering_safe():
    from hepconduit.filtering import compile_filter_fn

    ef = hepconduit.read(LHE_FILE)
    fn = compile_filter_fn("n_leptons >= 1 and ht > 0")
    assert fn(ef.events[0]) is True

    with pytest.raises(Exception):
        compile_filter_fn("__import__('os').system('echo pwned')")


def test_convert_lhe_to_hepmc_and_back(tmp_path: Path):
    out_h = tmp_path / "out.hepmc"
    out_l = tmp_path / "back.lhe"

    hepconduit.convert(LHE_FILE, out_h, quiet=True)
    ef_h = hepconduit.read(out_h)
    assert len(ef_h.events) == 3

    hepconduit.convert(out_h, out_l, quiet=True)
    ef_l = hepconduit.read(out_l)
    assert len(ef_l.events) == 3


def test_cli_info(tmp_path: Path):
    # run module as script via -m so it uses local package
    cmd = ["python", "-m", "hepconduit.cli", "info", str(LHE_FILE), "--json"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0
    assert '"n_events"' in res.stdout
