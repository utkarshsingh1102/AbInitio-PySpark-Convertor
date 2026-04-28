"""SORT → df.orderBy(...)."""

from __future__ import annotations

from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.registry import register


def _parse_keys(raw: object) -> list[tuple[str, str]]:
    """Returns a list of (column, direction) where direction is 'asc' or 'desc'.

    Accepts shapes like:
        "id"
        "id; name desc"
        ["id asc", "name desc"]
        [{"name": "id", "order": "desc"}, ...]
    """
    items: list[str | dict[str, object]]
    if isinstance(raw, list):
        items = raw  # type: ignore[assignment]
    elif isinstance(raw, str):
        items = [tok.strip() for tok in raw.replace(";", ",").split(",") if tok.strip()]
    else:
        raise MappingError(f"unparseable sort key: {raw!r}")

    parsed: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            col = str(item.get("name") or item.get("column") or "")
            direction = str(item.get("order") or item.get("direction") or "asc").lower()
        else:
            tokens = str(item).split()
            col = tokens[0]
            direction = (tokens[1].lower() if len(tokens) > 1 else "asc")
        if not col:
            raise MappingError(f"missing column name in sort key: {item!r}")
        if direction not in {"asc", "desc"}:
            direction = "asc"
        parsed.append((col, direction))
    if not parsed:
        raise MappingError("SORT requires at least one key")
    return parsed


@register("SORT")
def map_sort(comp: Component, inputs: list[str]) -> Op:
    if len(inputs) != 1:
        raise MappingError(f"SORT expects 1 input, got {len(inputs)}")
    raw = comp.params.get("key") or comp.params.get("keys") or comp.params.get("sort_key")
    if raw is None:
        raise MappingError(f"SORT {comp.id!r} has no `key` param")
    keys = _parse_keys(raw)
    args = ", ".join(f'F.col("{c}").{d}()' for c, d in keys)
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=f"{inputs[0]}.orderBy({args})",
    )
