"""Event filtering.

Filter expressions are parsed with `ast` and evaluated in a restricted
environment (no attribute access, no indexing, no imports).

Example:
    n_jets >= 2 and ht > 200

Available variables:
    n_particles, n_final, n_incoming, weight, process_id, scale,
    alpha_qed, alpha_qcd, n_jets, n_leptons, n_photons, n_neutrinos,
    ht, met
"""

from __future__ import annotations

import ast
import math
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping

from .models import Event

ALLOWED_FUNCS: dict[str, Callable[..., Any]] = {
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
    "sqrt": math.sqrt,
    "log": math.log,
    "exp": math.exp,
}

ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Call,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
)


class UnsafeFilterExpression(ValueError):
    pass


@dataclass(frozen=True)
class CompiledFilter:
    expr: str
    tree: ast.Expression


def _validate_ast(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ALLOWED_NODES):
            raise UnsafeFilterExpression(f"Disallowed syntax: {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise UnsafeFilterExpression("Dunder names are not allowed")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise UnsafeFilterExpression("Only simple function calls are allowed")
            if node.func.id not in ALLOWED_FUNCS:
                raise UnsafeFilterExpression(f"Function not allowed: {node.func.id}")


def compile_filter(expr: str) -> CompiledFilter:
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise UnsafeFilterExpression(str(e)) from e
    _validate_ast(tree)
    return CompiledFilter(expr=expr, tree=tree)  # type: ignore[arg-type]


def _compute_filter_variables(event: Event) -> dict[str, Any]:
    final = event.final_particles
    incoming = event.incoming_particles

    n_jets = 0
    n_leptons = 0
    n_photons = 0
    n_neutrinos = 0
    ht = 0.0
    met_x = 0.0
    met_y = 0.0

    for p in final:
        aid = abs(p.pdg_id)
        pt = p.pt

        if 1 <= aid <= 6 or aid == 21:
            n_jets += 1
            ht += pt
        elif aid in (11, 13, 15):
            n_leptons += 1
            ht += pt
        elif aid in (12, 14, 16):
            n_neutrinos += 1
            met_x += p.px
            met_y += p.py
        elif aid == 22:
            n_photons += 1
            ht += pt
        else:
            ht += pt

    met = math.sqrt(met_x * met_x + met_y * met_y)

    return {
        "n_particles": len(event.particles),
        "n_final": len(final),
        "n_incoming": len(incoming),
        "weight": event.weight,
        "process_id": event.process_id,
        "scale": event.scale,
        "alpha_qed": event.alpha_qed,
        "alpha_qcd": event.alpha_qcd,
        "n_jets": n_jets,
        "n_leptons": n_leptons,
        "n_photons": n_photons,
        "n_neutrinos": n_neutrinos,
        "ht": ht,
        "met": met,
    }


def eval_filter(compiled: CompiledFilter, env: Mapping[str, Any]) -> bool:
    code = compile(compiled.tree, "<hepconduit-filter>", "eval")
    safe_globals = {"__builtins__": {}, **ALLOWED_FUNCS}
    return bool(eval(code, safe_globals, dict(env)))


def compile_filter_fn(expr: str) -> Callable[[Event], bool]:
    compiled = compile_filter(expr)

    def _fn(event: Event) -> bool:
        return eval_filter(compiled, _compute_filter_variables(event))

    return _fn


def filter_events(events: Iterable[Event], expression: str) -> Iterator[Event]:
    fn = compile_filter_fn(expression)
    for ev in events:
        if fn(ev):
            yield ev
