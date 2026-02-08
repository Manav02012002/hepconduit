from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

from .models import Event
from .provenance import stable_json_dumps


@dataclass
class LossCounter:
    dropped_fields: Dict[str, int] = field(default_factory=dict)
    dropped_weights: int = 0
    dropped_runinfo_keys: Dict[str, int] = field(default_factory=dict)
    loss_examples: Dict[str, list[dict]] = field(default_factory=dict)


_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "lhe": {
        "particle_fields": {
            "pdg_id", "status", "mother1", "mother2", "color1", "color2", "px", "py", "pz", "energy", "mass", "spin"
        },
        "event_fields": {"event_number", "weights", "process_id", "scale", "alpha_qed", "alpha_qcd"},
        "run_fields": {"beam_pdg_id", "beam_energy", "weight_names", "processes", "generator_name", "generator_version", "extra"},
    },
    "hepmc3": {
        # Full Asciiv3 support (units, weights, vertex graph, barcodes)
        "particle_fields": {"pdg_id", "status", "px", "py", "pz", "energy", "mass", "barcode", "vertex_barcode", "end_vertex_barcode", "attributes"},
        "event_fields": {"event_number", "weights", "extra"},
        "run_fields": {"beam_pdg_id", "beam_energy", "weight_names", "generator_name", "generator_version", "extra"},
    },
    "csv": {
        "particle_fields": {"pdg_id", "status", "mother1", "mother2", "color1", "color2", "px", "py", "pz", "energy", "mass", "spin", "barcode", "vertex_barcode", "end_vertex_barcode"},
        "event_fields": {"event_number"},
        "run_fields": set(),
    },
    "tsv": {
        "particle_fields": {"pdg_id", "status", "mother1", "mother2", "color1", "color2", "px", "py", "pz", "energy", "mass", "spin", "barcode", "vertex_barcode", "end_vertex_barcode"},
        "event_fields": {"event_number"},
        "run_fields": set(),
    },
    "parquet": {
        # Full-fidelity Parquet schema (flat or columnar): particles + vertices + weights + run info.
        "particle_fields": {"pdg_id", "status", "mother1", "mother2", "color1", "color2", "px", "py", "pz", "energy", "mass", "spin", "barcode", "vertex_barcode", "end_vertex_barcode", "attributes"},
        "event_fields": {"event_number", "weights", "process_id", "scale", "alpha_qed", "alpha_qcd", "extra"},
        "run_fields": {"beam_pdg_id", "beam_energy", "weight_names", "processes", "generator_name", "generator_version", "extra"},
    },
}


def format_capabilities(fmt: str) -> Dict[str, Any]:
    return _CAPABILITIES.get(fmt, {"particle_fields": set(), "event_fields": set(), "run_fields": set()})


def loss_plan(input_format: str, output_format: str) -> Dict[str, Any]:
    ic = format_capabilities(input_format)
    oc = format_capabilities(output_format)
    dropped_particle = sorted(set(ic.get("particle_fields", set())) - set(oc.get("particle_fields", set())))
    dropped_event = sorted(set(ic.get("event_fields", set())) - set(oc.get("event_fields", set())))
    dropped_run = sorted(set(ic.get("run_fields", set())) - set(oc.get("run_fields", set())))
    return {
        "input_format": input_format,
        "output_format": output_format,
        "dropped_particle_fields": dropped_particle,
        "dropped_event_fields": dropped_event,
        "dropped_run_fields": dropped_run,
    }


def observe_losses(events: Iterable[Event], plan: Dict[str, Any]) -> tuple[Iterable[Event], LossCounter]:
    """Wrap an event iterator and count non-default values that will be dropped."""

    dropped_particle = set(plan.get("dropped_particle_fields", []))
    dropped_event = set(plan.get("dropped_event_fields", []))
    counter = LossCounter()

    def _non_default(val) -> bool:
        if val is None:
            return False
        if isinstance(val, (int, float)):
            return val != 0 and val != 0.0 and val != 9.0
        if isinstance(val, (list, tuple, dict, set)):
            return len(val) != 0
        if isinstance(val, str):
            return val != ""
        return True

    def _record_example(key: str, ex: dict) -> None:
        lst = counter.loss_examples.get(key)
        if lst is None:
            lst = []
            counter.loss_examples[key] = lst
        if len(lst) < 5:
            lst.append(ex)

    def _wrapped():
        for ev in events:
            # event-level
            if "weights" in dropped_event and _non_default(ev.weights) and len(ev.weights) > 1:
                counter.dropped_weights += 1
            for field in dropped_event:
                if field == "weights":
                    continue
                if hasattr(ev, field) and _non_default(getattr(ev, field)):
                    counter.dropped_fields[f"event.{field}"] = counter.dropped_fields.get(f"event.{field}", 0) + 1
                    _record_example(f"event.{field}", {"event": ev.event_number})

            # particle-level
            for p in ev.particles:
                for pf in dropped_particle:
                    if hasattr(p, pf) and _non_default(getattr(p, pf)):
                        k = f"particle.{pf}"
                        counter.dropped_fields[k] = counter.dropped_fields.get(k, 0) + 1
                        _record_example(k, {"event": ev.event_number, "particle_barcode": getattr(p, "barcode", 0) or 0})
            yield ev

    return _wrapped(), counter


def loss_hash(plan: Dict[str, Any], counter: LossCounter) -> str:
    payload = {
        "plan": plan,
        "observed": {
            "dropped_fields": counter.dropped_fields,
            "dropped_weights": counter.dropped_weights,
            "dropped_runinfo_keys": counter.dropped_runinfo_keys,
            "loss_examples": counter.loss_examples,
        },
    }
    s = stable_json_dumps(payload)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def conversion_report_to_sarif(report: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a hepconduit conversion report to SARIF 2.1.0.

    This enables CI annotations (GitHub, Azure DevOps, etc.) without inventing
    a bespoke format. The SARIF output is *deterministic* given the report.

    We intentionally keep the mapping conservative:
    - Each dropped field class becomes a SARIF result
    - Dropped multi-weights is surfaced as a warning
    """

    kind = report.get("kind")
    if kind != "hepconduit.conversion_report.v1":
        raise ValueError(f"Unsupported report kind for SARIF: {kind!r}")

    prov = report.get("provenance", {}) or {}
    inp = ((prov.get("input") or {}).get("path")) or "<input>"
    outp = ((prov.get("output") or {}).get("path")) or "<output>"

    plan = report.get("loss_plan", {}) or {}
    obs = report.get("observed", {}) or {}

    dropped_fields: Dict[str, int] = obs.get("dropped_fields", {}) or {}
    dropped_weights_events = int(obs.get("dropped_weights_events", 0) or 0)

    rules = []
    results = []

    def _add_rule(rule_id: str, name: str, short: str, full: str, severity: str):
        rules.append(
            {
                "id": rule_id,
                "name": name,
                "shortDescription": {"text": short},
                "fullDescription": {"text": full},
                "defaultConfiguration": {"level": severity},
            }
        )

    # Rules
    _add_rule(
        "HEPLOSS001",
        "DroppedField",
        "Some information cannot be represented in the output format.",
        "During conversion, some fields cannot be represented in the chosen output format and will be dropped. The conversion report includes an explicit loss plan and observed occurrences.",
        "warning",
    )
    _add_rule(
        "HEPLOSS002",
        "DroppedMultiWeights",
        "Multiple event weights cannot be represented in the output format.",
        "The output format does not support multiple named weights per event. Only the nominal weight may be retained.",
        "warning",
    )

    # Results
    for field, count in sorted(dropped_fields.items()):
        results.append(
            {
                "ruleId": "HEPLOSS001",
                "level": "warning",
                "message": {
                    "text": f"Dropped non-default values for {field} in {count} occurrences when converting {plan.get('input_format')} -> {plan.get('output_format')}."
                },
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": inp},
                        }
                    }
                ],
                "properties": {
                    "field": field,
                    "count": count,
                    "output": outp,
                },
            }
        )

    if dropped_weights_events:
        results.append(
            {
                "ruleId": "HEPLOSS002",
                "level": "warning",
                "message": {
                    "text": f"Dropped multi-weights in {dropped_weights_events} events when converting {plan.get('input_format')} -> {plan.get('output_format')}."
                },
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": inp},
                        }
                    }
                ],
                "properties": {
                    "count": dropped_weights_events,
                    "output": outp,
                },
            }
        )

    tool_name = str(prov.get("tool") or "hepconduit")
    tool_version = str(prov.get("tool_version") or "unknown")
    git_sha = prov.get("git_sha")

    # SARIF document
    return {
        "$schema": "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": tool_name,
                        "version": tool_version,
                        "informationUri": "https://pypi.org/project/hepconduit/",
                        "rules": rules,
                    }
                },
                "invocations": [
                    {
                        "executionSuccessful": True,
                        "properties": {
                            "git_sha": git_sha,
                            "loss_hash": report.get("loss_hash"),
                        },
                    }
                ],
                "results": results,
            }
        ],
    }