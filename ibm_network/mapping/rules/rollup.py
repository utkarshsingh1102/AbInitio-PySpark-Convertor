"""ROLLUP → df.groupBy(*keys).agg(*aggs).

Aggregate definitions live in the component's transform, e.g.:

    out.total_amount  :: sum(in.amount);
    out.order_count   :: count_recs();
"""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register
from ibm_network.mapping.transform_expr import transform_block_to_select_args
from ibm_network.mapping.transform_expr.codegen import TransformExprError


def _split_keys(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(k).strip() for k in raw if str(k).strip()]
    if isinstance(raw, str):
        return [k.strip() for k in raw.replace(",", " ").replace(";", " ").split() if k.strip()]
    raise MappingError(f"unparseable rollup key: {raw!r}")


@register("ROLLUP")
def map_rollup(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) != 1:
        raise MappingError(f"ROLLUP expects 1 input, got {len(inputs)}")
    raw_keys = comp.params.get("key") or comp.params.get("keys") or comp.params.get("rollup_key")
    if raw_keys is None:
        raise MappingError(f"ROLLUP {comp.id!r} has no `key` param")
    keys = _split_keys(raw_keys)

    transform = comp.transform or comp.params.get("transform")
    if not transform:
        raise MappingError(f"ROLLUP {comp.id!r} has no transform body")
    try:
        agg_args = transform_block_to_select_args(transform)
    except TransformExprError as e:
        raise MappingError(f"ROLLUP {comp.id!r}: {e}") from e

    keys_src = ", ".join(f'"{k}"' for k in keys)
    aggs_src = ", ".join(agg_args)
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f"{inputs[0]}.groupBy({keys_src}).agg({aggs_src})",
    )
