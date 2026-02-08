from pathlib import Path

import hepconduit


def test_convert_writes_audit_report(tmp_path: Path):
    inp = Path(__file__).parent / "fixtures" / "pp_to_wpenu.lhe"
    out = tmp_path / "out.hepmc"
    rep_path = Path(str(out) + ".hepconduit.json")

    res = hepconduit.convert(inp, out, quiet=True)
    assert res["n_output"] == 3
    assert rep_path.exists()
    report = res.get("report")
    assert report and report["kind"] == "hepconduit.conversion_report.v1"
    assert "provenance" in report
    assert report["provenance"]["input"]["sha256"]


def test_convert_writes_sarif_when_requested(tmp_path: Path):
    inp = Path(__file__).parent / "fixtures" / "pp_to_wpenu.lhe"
    out = tmp_path / "out.hepmc"
    rep_path = Path(str(out) + ".hepconduit.sarif")

    hepconduit.convert(inp, out, quiet=True, report_format="sarif")
    assert rep_path.exists()
    sarif = rep_path.read_text(encoding="utf-8")
    assert '"version":"2.1.0"' in sarif


def test_diff_and_contract_roundtrip(tmp_path: Path):
    inp = Path(__file__).parent / "fixtures" / "pp_to_wpenu.lhe"
    hepmc = tmp_path / "mid.hepmc"
    back = tmp_path / "back.lhe"
    hepconduit.convert(inp, hepmc, quiet=True)
    hepconduit.convert(hepmc, back, quiet=True)

    from hepconduit.diff import diff_files
    s = diff_files(str(inp), str(back), by="fingerprint")
    assert s["common"] == 3
    assert s["added"] == 0
    assert s["removed"] == 0

    from hepconduit.contracts import run_contract
    cr = run_contract(str(inp), contract="roundtrip_v1", to_format="hepmc3", strict=False)
    assert cr.ok, cr.details
