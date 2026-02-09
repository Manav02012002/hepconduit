"""Test fixtures.

Historically this repo shipped small LHE/HepMC3 fixtures under
``tests/fixtures/``. Some packaging / zip workflows can omit those files,
which makes the test suite fail for reasons unrelated to the code.

To make the test suite *portable*, we (re)generate deterministic fixtures
at test collection time if they are missing.
"""

from __future__ import annotations

from pathlib import Path


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _ensure_standard_fixtures(fixtures: Path) -> None:
    lhe = fixtures / "pp_to_wpenu.lhe"
    hepmc = fixtures / "pp_to_wpenu.hepmc"

    # A tiny, deterministic, *valid enough* LHE file with one event.
    if not lhe.exists():
        _write_text(
            lhe,
            """<LesHouchesEvents version="3.0">
<init>
2212 2212 6500 6500 0 0 0 0 0 0
1.0 0.0 1.0 1
</init>
<event>
4 1 1.0 91.1876 0.00729735256 0.118
  11 1 1 2 0 0  0.0 0.0 40.1895 40.1895 0.000511 0 9
 -11 1 1 2 0 0  0.0 0.0 -40.1895 40.1895 0.000511 0 9
  12 1 1 2 0 0  0.0 0.0 10.0 10.0 0.0 0 9
 -12 1 1 1 2 0 0  0.0 0.0 -10.0 10.0 0.0 0 9
</event>
</LesHouchesEvents>
""",
        )

    # A tiny HepMC3 Asciiv3 file with one event.
    if not hepmc.exists():
        _write_text(
            hepmc,
            """HepMC::Version 3.02.05
HepMC::Asciiv3-START_EVENT_LISTING
U GEV MM
E 1
W 1.0
V 1 0 0 0 0 0 2 1 2
P 1 11 1 0 0 40.1895 40.1895 0.000511 1 0
P 2 -11 1 0 0 -40.1895 40.1895 0.000511 1 0
HepMC::Asciiv3-END_EVENT_LISTING
""",
    )


def _ensure_edgecase_fixtures(fixtures: Path) -> None:
    """Fixtures that exercise common "messy" real-world quirks."""

    edge_lhe = fixtures / "edge_cases.lhe"
    _write_text(
        edge_lhe,
            """<LesHouchesEvents version="3.0">
<init>
# Comment lines inside init are common
2212 2212 6500 6500 0 0 0 0 0 0
</init>
<event>
# Header with fewer cols is seen in the wild
2 42 1.0
# Particle lines with Fortran D exponents and extra spaces
  22 1 0 0 0 0  0.0 0.0 1.0D+01 1.0D+01 0.0 0 9
  22 1 0 0 0 0  0.0 0.0 -1.0D+01 1.0D+01 0.0 0 9
# Generator-specific per-event trailer line(s) sometimes appear here
some_generator_token 123 4.5D+00
<weights>
  <weight id='nominal'> 1.0 </weight>
  <weight id='pdf_alt'> 0.8 </weight>
</weights>
<rwgt>
  <wgt id="mur=0.5_muf=0.5"> 0.9 </wgt>
  <wgt id="mur=2.0_muf=2.0"> 1.1 </wgt>
</rwgt>
</event>
</LesHouchesEvents>
""",
    )

    edge_hepmc = fixtures / "edge_cases.hepmc"
    _write_text(
        edge_hepmc,
            """HepMC::Version 3.02.05
HepMC::Asciiv3-START_EVENT_LISTING
U GEV MM
F generator=ExampleGen version=1.2.3
C xs=1.23 err=0.04 unit=pb
E 7
C 1.20 0.05 pb
A key1 value1
W 1.0 0.9 1.1
V 1 0 0 0 0 0 0
P 1 22 1 0 0 10 10 0 0 0
P 2 22 1 0 0 -10 10 0 0 0
HepMC::Asciiv3-END_EVENT_LISTING
""",
    )


def pytest_configure(config):  # noqa: D401
    """Ensure fixtures exist before any tests run."""

    root = Path(__file__).resolve().parent
    fixtures = root / "fixtures"
    _ensure_standard_fixtures(fixtures)
    _ensure_edgecase_fixtures(fixtures)
