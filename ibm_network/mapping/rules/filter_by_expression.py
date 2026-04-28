"""FILTER_BY_EXPRESSION → df.filter(<translated_expr>)."""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register
from ibm_network.mapping.transform_expr import expr_to_pyspark
from ibm_network.mapping.transform_expr.codegen import TransformExprError


@register("FILTER_BY_EXPRESSION")
def map_filter(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) != 1:
        raise MappingError(f"FILTER_BY_EXPRESSION expects 1 input, got {len(inputs)}")
    expr = comp.params.get("select_expr") or comp.params.get("expr")
    if not expr:
        raise MappingError(f"FILTER_BY_EXPRESSION {comp.id!r} has no select_expr param")
    try:
        cond_src = expr_to_pyspark(expr)
    except TransformExprError as e:
        raise MappingError(f"FILTER_BY_EXPRESSION {comp.id!r}: {e}") from e
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f"{inputs[0]}.filter({cond_src})",
    )
