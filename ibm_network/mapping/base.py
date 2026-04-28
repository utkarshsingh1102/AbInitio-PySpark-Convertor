from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ibm_network.ir.models import Component


class MappingError(Exception):
    """Raised when a component cannot be mapped deterministically."""


@dataclass
class Op:
    """One step in the generated PySpark pipeline.

    `code` is a Python expression that, when assigned to `var`, produces a DataFrame.
    """

    component_id: str
    var: str
    code: str
    notes: list[str] = field(default_factory=list)


class ComponentMapper(Protocol):
    """Each handler is a callable that takes the IR component plus upstream variable
    names (already-emitted DataFrame variables) and returns an `Op`.
    """

    def __call__(self, comp: Component, inputs: list[str]) -> Op: ...


def df_var(component_id: str) -> str:
    """Conventional DataFrame variable name for a component."""
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in component_id)
    return f"df_{safe}"
