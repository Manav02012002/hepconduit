from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional

from ..models import Event, RunInfo


class Writer(ABC):
    @abstractmethod
    def write(self, path: str, events: Iterable[Event], run_info: Optional[RunInfo], **kwargs) -> None:
        ...
