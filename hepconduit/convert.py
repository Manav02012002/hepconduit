"""High-level conversion/read/write/info API."""

from __future__ import annotations

import itertools
import sys
from pathlib import Path
from typing import Optional, Union

from .filtering import filter_events
from .models import EventFile
from .validation import validate as validate_file
from .validation import validate_stream

from .io.registry import detect_format, get_reader, get_writer, register

from .audit import loss_plan, observe_losses, loss_hash
from .provenance import build_provenance, stable_json_dumps
from .plugins import load_plugins

# Ensure default handlers are registered
from .io.lhe import LHEReader, LHEWriter
from .io.hepmc3 import HepMC3Reader, HepMC3Writer
from .io.csv_tsv import CSVReader, CSVWriter
from .io.parquet import ParquetReader, ParquetWriter

register("lhe", lambda: LHEReader(), lambda: LHEWriter())
register("hepmc3", lambda: HepMC3Reader(), lambda: HepMC3Writer())
register("csv", lambda: CSVReader(delimiter=","), lambda: CSVWriter(delimiter=","))
register("tsv", lambda: CSVReader(delimiter="	"), lambda: CSVWriter(delimiter="	"))
register("parquet", lambda: ParquetReader(), lambda: ParquetWriter())

# Load optional plugins (entry points) after registering built-ins.
load_plugins()

# Load third-party plugins (entry points) if present
load_plugins()


def read(filepath: Union[str, Path], format: Optional[str] = None) -> EventFile:
    if format is None:
        format = detect_format(filepath)
    reader = get_reader(format)
    ef = reader.read(str(filepath))
    ef.format_name = format
    return ef


def write(
    filepath: Union[str, Path],
    event_file: EventFile,
    format: Optional[str] = None,
    **kwargs,
) -> None:
    if format is None:
        format = detect_format(filepath)
    writer = get_writer(format)
    writer.write(str(filepath), event_file.events, event_file.run_info, **kwargs)


def convert(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    *,
    input_format: Optional[str] = None,
    output_format: Optional[str] = None,
    filter_expr: Optional[str] = None,
    max_events: int = -1,
    validate: bool = False,
    momentum_tolerance: float = 1e-4,
    quiet: bool = False,
    strict_validation: bool = False,
    report: str = "auto",
    report_format: str = "json",
    provenance: str = "auto",
    **writer_kwargs,
) -> dict:
    """Convert between supported formats.

    This implementation is streaming: events are never materialised into a full list
    during conversion.
    """

    if input_format is None:
        input_format = detect_format(input_path)
    if output_format is None:
        output_format = detect_format(output_path)

    reader = get_reader(input_format)
    writer = get_writer(output_format)

    if not quiet:
        print(f"Reading {input_format}: {input_path}", file=sys.stderr)

    # Count input events with a separate pass (best-effort). This keeps the main
    # conversion pipeline streaming.
    n_input = -1
    try:
        base_iter = reader.iter_events(str(input_path))
        if max_events >= 0:
            base_iter = itertools.islice(base_iter, max_events)
        n_input = sum(1 for _ in base_iter)
    except Exception:
        n_input = -1

    # Build the streaming pipeline
    ev_iter = reader.iter_events(str(input_path))
    if max_events >= 0:
        ev_iter = itertools.islice(ev_iter, max_events)

    if filter_expr:
        if not quiet:
            print(f"  Applying filter: {filter_expr}", file=sys.stderr)
        ev_iter = filter_events(ev_iter, filter_expr)

    if validate:
        ev_iter = validate_stream(
            ev_iter,
            momentum_tolerance=momentum_tolerance,
            strict=strict_validation,
        )

    try:
        run_info = reader.read_run_info(str(input_path))
    except Exception:
        run_info = None

    # Loss accounting (capability-based plan + observed non-default values)
    plan = loss_plan(input_format, output_format)
    ev_iter, loss_counter = observe_losses(ev_iter, plan)

    if not quiet:
        print(f"Writing {output_format}: {output_path}", file=sys.stderr)

    n_output = 0

    def _counting(it):
        nonlocal n_output
        for ev in it:
            n_output += 1
            yield ev

    # Compute audit + provenance (deterministic hash over loss report)
    lhash = loss_hash(plan, loss_counter)
    argv = ["hepconduit", "convert", str(input_path), str(output_path)]
    prov = build_provenance(
        tool="hepconduit",
        tool_version=__import__("hepconduit").__version__,
        input_path=input_path,
        output_path=output_path,
        input_format=input_format,
        output_format=output_format,
        argv=argv,
        loss_hash=lhash,
    )

    audit_report = {
        "kind": "hepconduit.conversion_report.v1",
        "loss_plan": plan,
        "observed": {
            "dropped_fields": loss_counter.dropped_fields,
            "dropped_weights_events": loss_counter.dropped_weights,
            "dropped_runinfo_keys": loss_counter.dropped_runinfo_keys,
            "loss_examples": loss_counter.loss_examples,
        },
        "loss_hash": lhash,
        "provenance": prov,
    }

    # Embed provenance/metadata where supported (currently Parquet only)
    if provenance != "none":
        md = writer_kwargs.get("metadata", {}) or {}
        # Keep metadata keys short-ish for Parquet KV store.
        md["hepconduit_provenance"] = stable_json_dumps(prov)
        md["hepconduit_loss_hash"] = lhash
        md["hepconduit_report_kind"] = "hepconduit.conversion_report.v1"
        writer_kwargs["metadata"] = md

    writer.write(str(output_path), _counting(ev_iter), run_info, **writer_kwargs)

    n_filtered = 0
    if n_input >= 0 and filter_expr:
        n_filtered = max(0, n_input - n_output)

    if not quiet:
        if n_input >= 0:
            print(f"  Read {n_input} events", file=sys.stderr)
        print(f"  Wrote {n_output} events", file=sys.stderr)
        if filter_expr and n_input >= 0:
            print(f"  Filtered out {n_filtered} events", file=sys.stderr)

    # Report output
    if report_format not in {"json", "sarif"}:
        raise ValueError("report_format must be 'json' or 'sarif'")

    if report_format == "json":
        out_text = stable_json_dumps(audit_report) + "\n"
        auto_suffix = ".hepconduit.json"
    else:
        from .audit import conversion_report_to_sarif

        sarif = conversion_report_to_sarif(audit_report)
        out_text = stable_json_dumps(sarif) + "\n"
        auto_suffix = ".hepconduit.sarif"

    if report == "auto":
        rp = str(output_path) + auto_suffix
        Path(rp).write_text(out_text, encoding="utf-8")
    elif report == "-":
        print(out_text.rstrip("\n"), file=sys.stdout)
    elif report in ("none", "off", "false"):
        pass
    else:
        Path(report).write_text(out_text, encoding="utf-8")

    if provenance == "sidecar":
        pp = str(output_path) + ".hepconduit.provenance.json"
        Path(pp).write_text(stable_json_dumps(prov) + "\n", encoding="utf-8")

    return {
        "n_input": n_input,
        "n_output": n_output,
        "n_filtered": n_filtered,
        "validation": None,
        "report": audit_report,
    }


def info(filepath: Union[str, Path], format: Optional[str] = None) -> dict:
    if format is None:
        format = detect_format(filepath)
    reader = get_reader(format)

    try:
        run_info = reader.read_run_info(str(filepath))
    except Exception:
        run_info = None

    n_events = 0
    total_particles = 0
    pdg_counts: dict[int, int] = {}
    status_counts: dict[int, int] = {}

    for ev in reader.iter_events(str(filepath)):
        n_events += 1
        total_particles += len(ev.particles)
        for p in ev.particles:
            pdg_counts[p.pdg_id] = pdg_counts.get(p.pdg_id, 0) + 1
            status_counts[p.status] = status_counts.get(p.status, 0) + 1

    from .pdg import name as pdg_name

    top_pdg = sorted(pdg_counts.items(), key=lambda x: -x[1])[:20]
    top_named = [(pdg_name(pid), count) for pid, count in top_pdg]

    beam_pdg_id = (0, 0)
    beam_energy = (0.0, 0.0)
    generator = ""
    generator_version = ""
    processes = []
    weight_names = []
    if run_info is not None:
        beam_pdg_id = run_info.beam_pdg_id
        beam_energy = run_info.beam_energy
        generator = run_info.generator_name
        generator_version = run_info.generator_version
        processes = run_info.processes
        weight_names = run_info.weight_names

    return {
        "format": format,
        "n_events": n_events,
        "total_particles": total_particles,
        "avg_particles_per_event": total_particles / max(1, n_events),
        "beam_pdg_id": beam_pdg_id,
        "beam_energy": beam_energy,
        "generator": generator,
        "generator_version": generator_version,
        "n_processes": len(processes),
        "weight_names": weight_names,
        "top_particles": top_named,
        "status_counts": dict(sorted(status_counts.items())),
    }


def validate(path_or_event_file, **kwargs):
    if isinstance(path_or_event_file, (str, Path)):
        ef = read(path_or_event_file)
        return validate_file(ef, **kwargs)
    return validate_file(path_or_event_file, **kwargs)
