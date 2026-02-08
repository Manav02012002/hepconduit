"""hepconduit: Universal HEP event data format converter."""

from __future__ import annotations

__version__ = "0.3.0"

from .convert import convert, read, write, info
from .validation import validate
from .models import EventFile, Event, Particle, RunInfo, ProcessInfo

__all__ = [
    "__version__",
    "convert",
    "read",
    "write",
    "info",
    "validate",
    "EventFile",
    "Event",
    "Particle",
    "RunInfo",
    "ProcessInfo",
]
