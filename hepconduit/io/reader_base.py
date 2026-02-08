from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from ..models import Event, EventFile, RunInfo


class Reader(ABC):
    @abstractmethod
    def read(self, path: str) -> EventFile:
        ...

    @abstractmethod
    def iter_events(self, path: str) -> Iterator[Event]:
        ...

    def read_run_info(self, path: str) -> RunInfo:
        """Optional fast path; default loads via read()."""
        return self.read(path).run_info
