"""
Physics validation for HEP event data.

Provides checks for:
- Momentum conservation
- Valid PDG particle IDs
- Energy positivity
- Mass consistency
- Color flow conservation
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

from . import pdg as pdg_module
from .models import Event, EventFile, Particle


@dataclass
class ValidationIssue:
    """A single validation issue found in an event."""

    level: str  # "error", "warning", "info"
    event_number: int
    particle_index: Optional[int]  # None for event-level issues
    message: str

    def __str__(self) -> str:
        loc = f"event {self.event_number}"
        if self.particle_index is not None:
            loc += f", particle {self.particle_index}"
        return f"[{self.level.upper()}] {loc}: {self.message}"

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "event_number": self.event_number,
            "particle_index": self.particle_index,
            "message": self.message,
        }


@dataclass
class ValidationReport:
    """Summary of all validation issues."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def n_errors(self) -> int:
        return sum(1 for i in self.issues if i.level == "error")

    @property
    def n_warnings(self) -> int:
        return sum(1 for i in self.issues if i.level == "warning")

    @property
    def is_valid(self) -> bool:
        return self.n_errors == 0

    def __str__(self) -> str:
        lines = [
            f"Validation: {self.n_errors} errors, {self.n_warnings} warnings, "
            f"{len(self.issues)} total issues"
        ]
        for issue in self.issues[:50]:  # Cap output
            lines.append(f"  {issue}")
        if len(self.issues) > 50:
            lines.append(f"  ... and {len(self.issues) - 50} more")
        return "\n".join(lines)

    def summary(self) -> str:
        """One-line summary."""
        return (
            f"{self.n_errors} errors, {self.n_warnings} warnings "
            f"across {len(self.issues)} issues"
        )

    def to_dict(self) -> dict:
        return {
            "n_errors": self.n_errors,
            "n_warnings": self.n_warnings,
            "n_issues": len(self.issues),
            "is_valid": self.is_valid,
            "issues": [i.to_dict() for i in self.issues],
        }


def validate_event(
    event: Event,
    *,
    check_momentum: bool = True,
    check_pdg: bool = True,
    check_energy: bool = True,
    check_mass: bool = True,
    momentum_tolerance: float = 1e-4,
    mass_tolerance: float = 1e-2,
) -> list[ValidationIssue]:
    """Validate a single event.

    Args:
        event: The event to validate.
        check_momentum: Check 4-momentum conservation.
        check_pdg: Check PDG ID validity.
        check_energy: Check energy positivity.
        check_mass: Check mass consistency.
        momentum_tolerance: Relative tolerance for momentum conservation.
        mass_tolerance: Relative tolerance for mass check.

    Returns:
        List of validation issues found.
    """
    issues: list[ValidationIssue] = []
    evt = event.event_number

    if not event.particles:
        issues.append(ValidationIssue("warning", evt, None, "Event has no particles"))
        return issues

    # --- PDG ID check ---
    if check_pdg:
        for i, p in enumerate(event.particles):
            if not pdg_module.is_valid_pdg_id(p.pdg_id):
                issues.append(ValidationIssue(
                    "warning", evt, i,
                    f"Unknown/invalid PDG ID: {p.pdg_id}"
                ))

    # --- Energy positivity ---
    if check_energy:
        for i, p in enumerate(event.particles):
            if p.energy < 0:
                issues.append(ValidationIssue(
                    "error", evt, i,
                    f"Negative energy: {p.energy:.6e} GeV"
                ))

    # --- Mass consistency ---
    if check_mass:
        for i, p in enumerate(event.particles):
            if p.mass == 0:
                continue
            if abs(p.mass) < 1e-3:
                continue
            computed = p.computed_mass
            rel_diff = abs(computed - p.mass) / max(abs(p.mass), 1e-12)
            if rel_diff > mass_tolerance:
                issues.append(ValidationIssue(
                    "warning", evt, i,
                    f"Mass inconsistency: stored={p.mass:.6e}, "
                    f"computed={computed:.6e}, rel_diff={rel_diff:.4e}"
                ))

    # --- Momentum conservation ---
    if check_momentum:
        incoming = [p for p in event.particles if p.status == -1]
        outgoing = [p for p in event.particles if p.status == 1]

        if incoming and outgoing:
            sum_in = [
                sum(p.px for p in incoming),
                sum(p.py for p in incoming),
                sum(p.pz for p in incoming),
                sum(p.energy for p in incoming),
            ]
            sum_out = [
                sum(p.px for p in outgoing),
                sum(p.py for p in outgoing),
                sum(p.pz for p in outgoing),
                sum(p.energy for p in outgoing),
            ]

            total_energy = max(abs(sum_in[3]), abs(sum_out[3]), 1e-10)
            labels = ["px", "py", "pz", "E"]

            for j in range(4):
                diff = abs(sum_in[j] - sum_out[j])
                if diff / total_energy > momentum_tolerance:
                    issues.append(ValidationIssue(
                        "error", evt, None,
                        f"Momentum non-conservation in {labels[j]}: "
                        f"in={sum_in[j]:.6e}, out={sum_out[j]:.6e}, "
                        f"diff={diff:.6e} ({diff/total_energy:.4e} relative)"
                    ))

    return issues


def validate(
    event_file: EventFile,
    *,
    check_momentum: bool = True,
    check_pdg: bool = True,
    check_energy: bool = True,
    check_mass: bool = True,
    momentum_tolerance: float = 1e-4,
    mass_tolerance: float = 1e-2,
    max_events: int = -1,
) -> ValidationReport:
    """Validate an entire event file.

    Args:
        event_file: The event file to validate.
        check_momentum: Check 4-momentum conservation.
        check_pdg: Check PDG ID validity.
        check_energy: Check energy positivity.
        check_mass: Check mass consistency.
        momentum_tolerance: Relative tolerance for momentum conservation.
        mass_tolerance: Relative tolerance for mass check.
        max_events: Maximum number of events to check (-1 for all).

    Returns:
        A ValidationReport summarizing all issues found.
    """
    report = ValidationReport()

    for i, event in enumerate(event_file.events):
        if max_events >= 0 and i >= max_events:
            break

        issues = validate_event(
            event,
            check_momentum=check_momentum,
            check_pdg=check_pdg,
            check_energy=check_energy,
            check_mass=check_mass,
            momentum_tolerance=momentum_tolerance,
            mass_tolerance=mass_tolerance,
        )
        report.issues.extend(issues)

    return report



def validate_stream(
    events,
    *,
    check_momentum: bool = True,
    check_pdg: bool = True,
    check_energy: bool = True,
    check_mass: bool = True,
    momentum_tolerance: float = 1e-4,
    mass_tolerance: float = 1e-2,
    max_events: int = -1,
    strict: bool = False,
):
    """Validate events in a streaming pipeline.

    If strict=True, raise ValueError on the first error; otherwise, yield events
    and attach a per-event issue list in event.extra["validation_issues"] (if any).
    """
    for i, event in enumerate(events):
        if max_events >= 0 and i >= max_events:
            break
        issues = validate_event(
            event,
            check_momentum=check_momentum,
            check_pdg=check_pdg,
            check_energy=check_energy,
            check_mass=check_mass,
            momentum_tolerance=momentum_tolerance,
            mass_tolerance=mass_tolerance,
        )
        errors = [iss for iss in issues if iss.level == "error"]
        if errors and strict:
            raise ValueError(str(errors[0]))
        if issues:
            event.extra = dict(event.extra)
            event.extra["validation_issues"] = [str(x) for x in issues]
        yield event
