"""Mapping stage: Ab Initio component → PySpark op.

Importing this package eagerly registers the v1 component handlers.
"""

from ibm_network.mapping.base import MappingError, Op
from ibm_network.mapping.registry import map_component, registered_types
from ibm_network.mapping.rules import (  # noqa: F401  -- side-effect: registers handlers
    dedup_sorted,
    filter_by_expression,
    join,
    reformat,
    rollup,
    sort,
)

__all__ = ["MappingError", "Op", "map_component", "registered_types"]
