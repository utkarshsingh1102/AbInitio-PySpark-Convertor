"""JOIN → left.join(right, on=keys, how=<inner/left/right/full>)."""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register

_JOIN_TYPE_MAP = {
    "inner": "inner",
    "outer": "full_outer",
    "full": "full_outer",
    "full_outer": "full_outer",
    "left": "left",
    "left_outer": "left",
    "right": "right",
    "right_outer": "right",
    # Ab Initio "explicit" join → inner with semantics handled upstream
    "explicit": "inner",
}


def _split_keys(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(k).strip() for k in raw if str(k).strip()]
    if isinstance(raw, str):
        return [k.strip() for k in raw.replace(",", " ").split() if k.strip()]
    raise MappingError(f"unparseable join key: {raw!r}")


@register("JOIN")
def map_join(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) < 2:
        raise MappingError(f"JOIN expects ≥2 inputs, got {len(inputs)}")
    if len(inputs) > 2:
        raise MappingError(
            f"JOIN with {len(inputs)} inputs not supported in v1 (use chained joins)"
        )
    raw_keys = comp.params.get("key") or comp.params.get("join_key") or comp.params.get("keys")
    if not raw_keys:
        raise MappingError(f"JOIN {comp.id!r} has no `key` param")
    keys = _split_keys(raw_keys)
    join_type_raw = str(comp.params.get("join_type", "inner")).lower()
    join_type = _JOIN_TYPE_MAP.get(join_type_raw, "inner")
    keys_src = "[" + ", ".join(f'"{k}"' for k in keys) + "]"
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f'{inputs[0]}.join({inputs[1]}, on={keys_src}, how="{join_type}")',
    )
