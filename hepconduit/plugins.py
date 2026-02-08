from __future__ import annotations

from importlib import metadata
from typing import Callable

from .io.registry import register


_LOADED = False


def load_plugins() -> None:
    """Load hepconduit plugins via Python entry points.

    Supported entry-point groups:
      - hepconduit.formats: callables that return (fmt, reader_factory, writer_factory)

    This keeps core lightweight while allowing experiments to extend formats
    without forking.
    """

    global _LOADED
    if _LOADED:
        return
    _LOADED = True

    try:
        eps = metadata.entry_points()
        group = eps.select(group="hepconduit.formats") if hasattr(eps, "select") else eps.get("hepconduit.formats", [])
    except Exception:
        return

    for ep in group:
        try:
            fn = ep.load()
            fmt, reader_factory, writer_factory = fn()
            register(fmt, reader_factory, writer_factory)
        except Exception:
            # Plugin failures must not break core functionality.
            continue
