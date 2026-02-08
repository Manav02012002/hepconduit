"""
Core event data model for hepconduit.

This module defines the internal representation that all formats convert
to and from. The design captures the superset of information across
LHE, HepMC3, ROOT ntuples, Parquet, and CSV formats.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Particle:
    """A single particle in an event.

    Attributes:
        pdg_id: PDG Monte Carlo particle ID.
        status: Status code. Convention:
            -1  = incoming
             1  = final state (stable outgoing)
             2  = intermediate (decayed resonance)
             3  = documentation line
            HepMC uses richer status codes (e.g. 4=beam).
        px, py, pz, energy: Four-momentum components in GeV.
        mass: Invariant mass in GeV. If not provided, computed from 4-momentum.
        mother1, mother2: 1-based indices of mother particles (0 = no mother).
        color1, color2: Color flow tags (LHE convention).
        spin: Cosine of the angle between the spin vector and the 3-momentum
              of the decaying particle (LHE lifetime/spin column).
        barcode: Unique particle identifier within an event (HepMC).
        vertex_barcode: Production vertex barcode (HepMC).
    """

    pdg_id: int
    status: int
    px: float
    py: float
    pz: float
    energy: float
    mass: float = 0.0
    mother1: int = 0
    mother2: int = 0
    color1: int = 0
    color2: int = 0
    spin: float = 9.0  # 9.0 = unknown (LHE convention)
    barcode: int = 0
    vertex_barcode: int = 0
    end_vertex_barcode: int = 0
    attributes: dict = field(default_factory=dict)

    @property
    def pt(self) -> float:
        """Transverse momentum."""
        return math.sqrt(self.px**2 + self.py**2)

    @property
    def eta(self) -> float:
        """Pseudorapidity."""
        p = math.sqrt(self.px**2 + self.py**2 + self.pz**2)
        if p == abs(self.pz):
            return float("inf") if self.pz >= 0 else float("-inf")
        return 0.5 * math.log((p + self.pz) / (p - self.pz))

    @property
    def phi(self) -> float:
        """Azimuthal angle."""
        return math.atan2(self.py, self.px)

    @property
    def rapidity(self) -> float:
        """Rapidity."""
        if self.energy == abs(self.pz):
            return float("inf") if self.pz >= 0 else float("-inf")
        return 0.5 * math.log((self.energy + self.pz) / (self.energy - self.pz))

    @property
    def computed_mass(self) -> float:
        """Mass computed from four-momentum.

        Numerically, m^2 = E^2 - |p|^2 can drift slightly negative for
        ultra-relativistic / tiny-mass particles. We clamp small negative
        values to zero to avoid nonsense masses.
        """
        m2 = self.energy**2 - self.px**2 - self.py**2 - self.pz**2
        if m2 < 0 and abs(m2) < 1e-8:
            m2 = 0.0
        return math.sqrt(m2) if m2 >= 0 else -math.sqrt(-m2)

    @property
    def is_incoming(self) -> bool:
        return self.status == -1

    @property
    def is_final(self) -> bool:
        return self.status == 1

    @property
    def is_intermediate(self) -> bool:
        return self.status == 2

    def to_dict(self) -> dict:
        """Convert to a flat dictionary for tabular formats."""
        return {
            "pdg_id": self.pdg_id,
            "status": self.status,
            "mother1": self.mother1,
            "mother2": self.mother2,
            "color1": self.color1,
            "color2": self.color2,
            "px": self.px,
            "py": self.py,
            "pz": self.pz,
            "energy": self.energy,
            "mass": self.mass,
            "spin": self.spin,
            "barcode": self.barcode,
            "vertex_barcode": self.vertex_barcode,
            "end_vertex_barcode": self.end_vertex_barcode,
        }


@dataclass
class Vertex:
    """A vertex in the event record (HepMC-style).

    Vertices are the points where particles interact. In LHE, vertices
    are implicit from the mother/daughter relationships.

    Attributes:
        barcode: Unique vertex identifier (negative integer by convention).
        x, y, z, t: Spacetime position of the vertex (mm, mm/c).
        incoming: Barcodes of incoming particles.
        outgoing: Barcodes of outgoing particles.
    """

    barcode: int = 0
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    t: float = 0.0
    incoming: list[int] = field(default_factory=list)
    outgoing: list[int] = field(default_factory=list)


@dataclass
class ProcessInfo:
    """Information about a physics process.

    Attributes:
        process_id: Process identifier (IDPRUP in LHE).
        cross_section: Cross section in pb.
        cross_section_error: Statistical error on the cross section.
        max_weight: Maximum weight for unweighting.
    """

    process_id: int = 0
    cross_section: float = 0.0
    cross_section_error: float = 0.0
    max_weight: float = 0.0


@dataclass
class RunInfo:
    """Run-level metadata.

    Attributes:
        beam_pdg_id: Tuple of (beam1, beam2) PDG IDs.
        beam_energy: Tuple of (beam1, beam2) energies in GeV.
        weight_names: List of weight names for multi-weight events.
        processes: List of ProcessInfo for all processes in the run.
        generator_name: Name of the generator that produced the events.
        generator_version: Generator version string.
        extra: Dictionary of additional metadata.
    """

    beam_pdg_id: tuple[int, int] = (0, 0)
    beam_energy: tuple[float, float] = (0.0, 0.0)
    weight_names: list[str] = field(default_factory=list)
    processes: list[ProcessInfo] = field(default_factory=list)
    generator_name: str = ""
    generator_version: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class Event:
    """A single physics event.

    Attributes:
        event_number: Sequential event number.
        particles: List of particles in the event.
        vertices: List of vertices (HepMC style).
        weights: Event weights. First element is the main weight.
        process_id: Process identifier.
        scale: Factorization/renormalization scale in GeV.
        alpha_qed: QED coupling alpha at the event scale.
        alpha_qcd: QCD coupling alpha_s at the event scale.
        n_particles: Number of particles (may differ from len(particles)
                     if some are documentation-only).
        extra: Dictionary of additional per-event metadata.
    """

    event_number: int = 0
    particles: list[Particle] = field(default_factory=list)
    vertices: list[Vertex] = field(default_factory=list)
    weights: list[float] = field(default_factory=lambda: [1.0])
    process_id: int = 0
    scale: float = 0.0
    alpha_qed: float = 0.0
    alpha_qcd: float = 0.0
    n_particles: int = 0
    extra: dict = field(default_factory=dict)

    @property
    def weight(self) -> float:
        """Primary event weight."""
        return self.weights[0] if self.weights else 1.0

    @property
    def incoming_particles(self) -> list[Particle]:
        return [p for p in self.particles if p.is_incoming]

    @property
    def final_particles(self) -> list[Particle]:
        return [p for p in self.particles if p.is_final]

    @property
    def intermediate_particles(self) -> list[Particle]:
        return [p for p in self.particles if p.is_intermediate]

    @property
    def n_final(self) -> int:
        return len(self.final_particles)


@dataclass
class EventFile:
    """A complete event file with run info and events.

    This is the top-level container that all readers produce and all
    writers consume. For streaming large files, use the reader/writer
    classes directly with their iterator interfaces.

    Attributes:
        run_info: Run-level metadata.
        events: List of events.
        format_name: Name of the original format (e.g., "lhe", "hepmc3").
    """

    run_info: RunInfo = field(default_factory=RunInfo)
    events: list[Event] = field(default_factory=list)
    format_name: str = ""

    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self):
        return iter(self.events)

    def __getitem__(self, idx):
        return self.events[idx]
