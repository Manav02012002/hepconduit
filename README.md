# hepconduit

**Universal HEP event data format converter — like pandoc for particle physics.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`hepconduit` converts between High Energy Physics event data formats with a single command. It handles LHE, HepMC3, CSV/TSV, and Apache Parquet, with built-in physics validation, event filtering, and streaming support for large files.

## Installation

```bash
pip install hepconduit            # core (LHE, HepMC3, CSV)
pip install hepconduit[parquet]   # + Parquet support via pyarrow
pip install hepconduit[all]       # everything
```

## Quick Start

### CLI

```bash
# Convert LHE to Parquet
hepconduit convert events.lhe events.parquet

# Convert with filtering
hepconduit convert events.lhe filtered.parquet --filter "n_jets >= 2 and ht > 200"

# Inspect a file
hepconduit info events.lhe

# Validate physics consistency
hepconduit validate events.lhe
```

### Python API

```python
import hepconduit

# One-line conversion
hepconduit.convert("events.lhe", "events.parquet")

# Read, manipulate, write
event_file = hepconduit.read("events.lhe")
for event in event_file:
    print(f"Event {event.event_number}: {event.n_final} final-state particles")

# Filter and write
from hepconduit import filter_events
filtered = list(filter_events(iter(event_file.events), "n_leptons == 2"))
event_file.events = filtered
hepconduit.write("dileptons.hepmc", event_file)

# Validate
report = hepconduit.validate(event_file)
print(report)
```

## Supported Formats

| Format | Extensions | Read | Write | Streaming |
|--------|-----------|------|-------|-----------|
| **LHE** (Les Houches Events) | `.lhe`, `.lhe.gz` | ✅ | ✅ | ✅ |
| **HepMC3** (ASCII) | `.hepmc`, `.hepmc.gz` | ✅ | ✅ | ✅ |
| **CSV/TSV** | `.csv`, `.tsv` | ✅ | ✅ | ✅ |
| **Apache Parquet** | `.parquet` | ✅ | ✅ | — |

All formats support transparent gzip compression (`.gz` suffix).

## Features

### Audited conversion (loss report + provenance)

Every conversion can emit a deterministic, machine-readable report of what was preserved, mapped, or dropped
between formats, plus full provenance (tool version, git SHA, input hashes, argv, etc.).

```bash
hepconduit convert input.lhe out.hepmc --report auto --report-format json
hepconduit convert input.lhe out.hepmc --report auto --report-format sarif   # CI annotations
```

By default, `--report auto` writes a sidecar: `out.hepmc.hepconduit.json` (or `.sarif`).
For Parquet outputs, provenance and a loss hash are embedded into Parquet key-value metadata by default.

### Semantic diff

Compare two files by deterministic event fingerprints:

```bash
hepconduit diff A.lhe B.hepmc --by fingerprint --json
```

### Contracts / certification

Contract-driven checks that combine parse → validate → convert → re-parse → invariants:

```bash
hepconduit certify events.lhe --contract roundtrip_v1 --to hepmc3
```

### Schema tooling (Parquet)

```bash
hepconduit schema show
hepconduit schema upgrade in.parquet out.parquet --to hepconduit.event.v1.columnar
```

### Plugin system

External packages can register new formats without forking via Python entry points
(group: `hepconduit.formats`).

### Event Filtering

Filter events during conversion using Python expressions with access to physics variables:

```bash
hepconduit convert input.lhe output.parquet --filter "n_jets >= 2 and ht > 200"
```

Available variables: `n_particles`, `n_final`, `n_incoming`, `weight`, `process_id`, `scale`, `alpha_qed`, `alpha_qcd`, `n_jets`, `n_leptons`, `n_photons`, `n_neutrinos`, `ht`, `met`.

### Physics Validation

Check momentum conservation, energy positivity, PDG ID validity, and mass consistency:

```bash
hepconduit validate events.lhe --momentum-tolerance 1e-4
```

```python
report = hepconduit.validate(event_file)
print(f"Valid: {report.is_valid}")
print(f"Errors: {report.n_errors}, Warnings: {report.n_warnings}")
```

### File Inspection

```bash
$ hepconduit info events.lhe
Format:              lhe
Events:              10000
Total particles:     50000
Avg particles/event: 5.0
Beam PDG IDs:        (2212, 2212)
Beam energies:       (6500.0, 6500.0) GeV
Generator:           MadGraph5_aMC@NLO v2.9.18
Processes:           1
Status codes:        {-1: 20000, 1: 20000, 2: 10000}
Top particles:
                  u: 8000
                 d~: 7500
                 W+: 10000
                 e+: 10000
              nu_e: 10000
```

### Parquet Output Modes

Two schemas for Parquet output:

```bash
# Flat (particle-per-row, like CSV but faster)
hepconduit convert events.lhe events.parquet

# Columnar (event-per-row with list columns, natural for awkward-array)
hepconduit convert events.lhe events.parquet --columnar
```

## Data Model

The internal representation captures the superset of information across all formats:

```python
from hepconduit import Event, Particle, RunInfo

# Access particle kinematics
for particle in event.final_particles:
    print(f"  {particle.pdg_id}: pT={particle.pt:.1f} GeV, η={particle.eta:.2f}")

# Computed properties
print(f"Mass from 4-momentum: {particle.computed_mass:.4f} GeV")
print(f"Rapidity: {particle.rapidity:.4f}")
```

## Compared To

- **pylhe**: LHE-only reader. `hepconduit` reads and *writes* LHE plus 3 other formats.
- **pyhepmc**: HepMC-only. `hepconduit` is format-agnostic.
- **uproot**: ROOT-focused. `hepconduit` targets the generator/parton-level formats that uproot doesn't specialize in.

`hepconduit` is the glue between these tools — it converts from any supported format to any other.

## License

MIT
