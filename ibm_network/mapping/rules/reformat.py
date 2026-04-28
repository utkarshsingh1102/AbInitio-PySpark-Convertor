"""REFORMAT → df.select(*projected_columns)."""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register
from ibm_network.mapping.transform_expr import transform_block_to_select_args
from ibm_network.mapping.transform_expr.codegen import TransformExprError


@register("REFORMAT")
def map_reformat(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) != 1:
        raise MappingError(f"REFORMAT expects 1 input, got {len(inputs)}")
    transform = comp.transform or comp.params.get("transform")
    if not transform:
        raise MappingError(f"REFORMAT {comp.id!r} has no transform body")
    try:
        select_args = transform_block_to_select_args(transform)
    except TransformExprError as e:
        raise MappingError(f"REFORMAT {comp.id!r}: {e}") from e
    args_src = ", ".join(select_args)
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f"{inputs[0]}.select({args_src})",
    )
