"""
Command-line interface for hepconduit.

Usage:
    hepconduit convert input.lhe output.parquet [--filter "n_jets >= 2"]
    hepconduit info input.lhe
    hepconduit validate input.lhe
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import hepconduit

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hepconduit",
        description="Universal HEP event data format converter. "
        "Like pandoc for particle physics.",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {hepconduit.__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- convert ---
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert between HEP event formats",
        description="Convert between LHE, HepMC3, CSV, TSV, and Parquet formats.",
    )
    convert_parser.add_argument("input", help="Input file path")
    convert_parser.add_argument("output", help="Output file path")
    convert_parser.add_argument(
        "--from", dest="input_format", default=None,
        help="Input format (auto-detected from extension if omitted)",
    )
    convert_parser.add_argument(
        "--to", dest="output_format", default=None,
        help="Output format (auto-detected from extension if omitted)",
    )
    convert_parser.add_argument(
        "--filter", dest="filter_expr", default=None,
        help='Event filter expression, e.g. "n_jets >= 2 and ht > 200"',
    )
    convert_parser.add_argument(
        "--max-events", type=int, default=-1,
        help="Maximum number of events to convert (-1 for all)",
    )
    convert_parser.add_argument(
        "--validate", action="store_true",
        help="Run physics validation on input events",
    )
    convert_parser.add_argument(
        "--columnar", action="store_true",
        help="Use columnar (event-per-row with list columns) Parquet schema",
    )
    convert_parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress progress output",
    )

    convert_parser.add_argument(
        "--report",
        default="auto",
        help=(
            "Conversion audit report output. 'auto' writes <output>.hepconduit.<ext> where "
            "<ext> depends on --report-format; '-' writes the report to stdout; 'none' disables."
        ),
    )

    convert_parser.add_argument(
        "--report-format",
        choices=["json", "sarif"],
        default="json",
        help="Audit report format: 'json' (default) or 'sarif' (SARIF 2.1.0).",
    )

    convert_parser.add_argument(
        "--provenance",
        default="auto",
        help=(
            "Provenance embedding mode. 'auto' embeds provenance where supported "
            "(e.g., Parquet key-value metadata) and always includes it in the audit report; "
            "'sidecar' writes <output>.hepconduit.provenance.json; 'none' disables."
        ),
    )

    # --- info ---
    info_parser = subparsers.add_parser(
        "info",
        help="Show information about an event file",
    )
    info_parser.add_argument("input", help="Input file path")
    info_parser.add_argument(
        "--format", dest="input_format", default=None,
        help="Input format (auto-detected if omitted)",
    )
    info_parser.add_argument(
        "--json", dest="as_json", action="store_true",
        help="Output as JSON",
    )

    # --- validate ---
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate an event file for physics consistency",
    )
    validate_parser.add_argument("input", help="Input file path")
    validate_parser.add_argument(
        "--format", dest="input_format", default=None,
        help="Input format (auto-detected if omitted)",
    )
    validate_parser.add_argument(
        "--max-events", type=int, default=-1,
        help="Maximum number of events to validate (-1 for all)",
    )
    validate_parser.add_argument(
        "--momentum-tolerance", type=float, default=1e-4,
        help="Relative tolerance for momentum conservation (default: 1e-4)",
    )

    # --- diff ---
    diff_parser = subparsers.add_parser(
        "diff",
        help="Semantic diff between event files",
        description="Compare two event files with stable event fingerprints and summary statistics.",
    )
    diff_parser.add_argument("a", help="Path to file A")
    diff_parser.add_argument("b", help="Path to file B")
    diff_parser.add_argument(
        "--by",
        choices=["fingerprint", "index"],
        default="fingerprint",
        help="Match events by stable fingerprint (default) or by event order.",
    )
    diff_parser.add_argument(
        "--max-events",
        type=int,
        default=-1,
        help="Maximum number of events to compare (-1 for all)",
    )
    diff_parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Output diff summary as JSON",
    )

    # --- certify ---
    cert_parser = subparsers.add_parser(
        "certify",
        help="Run a conversion contract and certify invariants",
        description="Run a contract (parse/validate/convert/re-parse/invariants) and fail on violations.",
    )
    cert_parser.add_argument("input", help="Input file path")
    cert_parser.add_argument(
        "--pack",
        default=None,
        help="Contract pack name (runs multiple contracts). Use `hepconduit doctor` or docs for available packs.",
    )
    cert_parser.add_argument(
        "--contract",
        default="roundtrip_v1",
        help="Contract name (default: roundtrip_v1)",
    )
    cert_parser.add_argument(
        "--to",
        dest="to_format",
        default="hepmc3",
        help="Intermediate format for round-trip contracts (default: hepmc3)",
    )
    cert_parser.add_argument(
        "--strict",
        action="store_true",
        help="Use strict validation during certification",
    )
    cert_parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Output certification report as JSON",
    )

    # --- schema ---
    schema_parser = subparsers.add_parser(
        "schema",
        help="Inspect and manage Parquet schemas",
    )
    schema_sub = schema_parser.add_subparsers(dest="schema_cmd")
    schema_show = schema_sub.add_parser("show", help="Show known schema versions")
    schema_show.add_argument("--json", dest="as_json", action="store_true")
    schema_upgrade = schema_sub.add_parser("upgrade", help="Upgrade a Parquet file schema")
    schema_upgrade.add_argument("input", help="Input Parquet file")
    schema_upgrade.add_argument("output", help="Output Parquet file")
    schema_upgrade.add_argument("--to", dest="to_schema", default="hepconduit.event.v1.flat")

    # --- doctor ---
    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Environment & capability check",
    )
    doctor_parser.add_argument("--json", dest="as_json", action="store_true")

    return parser


def _cmd_convert(args: argparse.Namespace) -> int:
    from .convert import convert

    writer_kwargs = {}
    if args.columnar:
        writer_kwargs["columnar"] = True

    try:
        result = convert(
            args.input,
            args.output,
            input_format=args.input_format,
            output_format=args.output_format,
            filter_expr=args.filter_expr,
            max_events=args.max_events,
            validate=args.validate,
            quiet=args.quiet,
            report=args.report,
            report_format=args.report_format,
            provenance=args.provenance,
            **writer_kwargs,
        )
    except (ValueError, FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.validate and result.get("validation"):
        report = result["validation"]
        if not report.is_valid:
            print(str(report), file=sys.stderr)
            return 2  # Validation errors (but conversion completed)

    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    from .diff import diff_files

    try:
        summary = diff_files(
            args.a,
            args.b,
            by=args.by,
        )
    except (ValueError, FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.as_json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        from .diff import format_diff_human

        print(format_diff_human(summary))
    return 0


def _cmd_certify(args: argparse.Namespace) -> int:
    from .contracts import certify, certify_pack

    try:
        if args.pack:
            report = certify_pack(
                args.input,
                pack=args.pack,
                to_format=args.to_format,
                strict=args.strict,
            )
        else:
            report = certify(
                args.input,
                contract=args.contract,
                to_format=args.to_format,
                strict=args.strict,
            )
    except (ValueError, FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.as_json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(str(report))
    return 0 if report.ok else 2


def _cmd_schema(args: argparse.Namespace) -> int:
    from .schema import list_schemas, upgrade_parquet

    if args.schema_cmd is None:
        print("Error: missing schema subcommand (show|upgrade)", file=sys.stderr)
        return 1

    if args.schema_cmd == "show":
        schemas = list_schemas()
        if args.as_json:
            print(json.dumps(schemas, indent=2, sort_keys=True))
        else:
            for s in schemas["schemas"]:
                print(f"{s['id']}: {s['description']}")
        return 0

    if args.schema_cmd == "upgrade":
        try:
            upgrade_parquet(args.input, args.output, to_schema=args.to_schema)
        except (ValueError, FileNotFoundError, ImportError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        return 0

    print(f"Error: unknown schema subcommand: {args.schema_cmd}", file=sys.stderr)
    return 1


def _cmd_doctor(args: argparse.Namespace) -> int:
    from .doctor import doctor_report

    rep = doctor_report()
    if args.as_json:
        print(json.dumps(rep, indent=2, sort_keys=True))
    else:
        print(rep["summary"])
        for item in rep["checks"]:
            status = "OK" if item["ok"] else "FAIL"
            print(f"- {status}: {item['name']}: {item['detail']}")
    return 0 if all(c["ok"] for c in rep["checks"]) else 2


def _cmd_info(args: argparse.Namespace) -> int:
    from .convert import info

    try:
        result = info(args.input, format=args.input_format)
    except (ValueError, FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.as_json:
        # Make JSON-serializable
        serializable = {}
        for k, v in result.items():
            if isinstance(v, tuple):
                serializable[k] = list(v)
            else:
                serializable[k] = v
        print(json.dumps(serializable, indent=2))
    else:
        print(f"Format:              {result['format']}")
        print(f"Events:              {result['n_events']}")
        print(f"Total particles:     {result['total_particles']}")
        print(f"Avg particles/event: {result['avg_particles_per_event']:.1f}")

        if result['beam_pdg_id'] != (0, 0):
            print(f"Beam PDG IDs:        {result['beam_pdg_id']}")
            print(f"Beam energies:       {result['beam_energy']} GeV")

        if result['generator']:
            gen = result['generator']
            if result['generator_version']:
                gen += f" v{result['generator_version']}"
            print(f"Generator:           {gen}")

        if result['n_processes']:
            print(f"Processes:           {result['n_processes']}")

        if result['weight_names']:
            print(f"Weight names:        {result['weight_names'][:5]}")
            if len(result['weight_names']) > 5:
                print(f"                     ... and {len(result['weight_names']) - 5} more")

        if result['status_counts']:
            print(f"Status codes:        {result['status_counts']}")

        if result['top_particles']:
            print("Top particles:")
            for name, count in result['top_particles'][:10]:
                print(f"  {name:>20s}: {count}")

    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from .convert import read
    from .validation import validate

    try:
        event_file = read(args.input, format=args.input_format)
    except (ValueError, FileNotFoundError, ImportError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    report = validate(
        event_file,
        max_events=args.max_events,
        momentum_tolerance=args.momentum_tolerance,
    )

    print(str(report))
    return 0 if report.is_valid else 2


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "convert": _cmd_convert,
        "info": _cmd_info,
        "validate": _cmd_validate,
        "diff": _cmd_diff,
        "certify": _cmd_certify,
        "schema": _cmd_schema,
        "doctor": _cmd_doctor,
    }

    handler = commands.get(args.command)
    if handler is None:
        parser.print_help()
        return 1

    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
