from __future__ import annotations

from typing import Callable

from ibm_network.ir.models import Component
from ibm_network.mapping.base import ComponentMapper, MappingError, Op

_REGISTRY: dict[str, ComponentMapper] = {}


def register(ab_initio_type: str) -> Callable[[ComponentMapper], ComponentMapper]:
    key = ab_initio_type.upper()

    def decorator(fn: ComponentMapper) -> ComponentMapper:
        if key in _REGISTRY:
            raise ValueError(f"duplicate handler registered for {key!r}")
        _REGISTRY[key] = fn
        return fn

    return decorator


def map_component(comp: Component, inputs: list[str]) -> Op:
    handler = _REGISTRY.get(comp.ab_initio_type.upper())
    if handler is None:
        raise MappingError(f"no rule for component type: {comp.ab_initio_type!r}")
    return handler(comp, inputs)


def registered_types() -> list[str]:
    return sorted(_REGISTRY)


def has_rule(ab_initio_type: str) -> bool:
    return ab_initio_type.upper() in _REGISTRY
