"""DEDUP_SORTED → df.dropDuplicates([keys])."""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register


def _split_keys(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(k).strip() for k in raw if str(k).strip()]
    if isinstance(raw, str):
        return [k.strip() for k in raw.replace(",", " ").replace(";", " ").split() if k.strip()]
    raise MappingError(f"unparseable dedup key: {raw!r}")


@register("DEDUP_SORTED")
def map_dedup_sorted(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) != 1:
        raise MappingError(f"DEDUP_SORTED expects 1 input, got {len(inputs)}")
    raw = comp.params.get("key") or comp.params.get("keys") or comp.params.get("dedup_key")
    if not raw:
        raise MappingError(f"DEDUP_SORTED {comp.id!r} has no `key` param")
    keys = _split_keys(raw)
    keys_src = "[" + ", ".join(f'"{k}"' for k in keys) + "]"
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f"{inputs[0]}.dropDuplicates({keys_src})",
    )
