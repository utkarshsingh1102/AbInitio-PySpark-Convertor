"""One test per v1 component handler. Each asserts the emitted PySpark `code` is
syntactically valid Python and contains the expected operator."""

from __future__ import annotations

import ast

import pytest

from ibm_network.ir.models import Component
from ibm_network.mapping import map_component
from ibm_network.mapping.base import MappingError, df_var


def _make(comp_type: str, comp_id: str = "c1", **kw: object) -> Component:
    return Component(
        id=comp_id,
        ab_initio_type=comp_type,
        name=kw.pop("name", comp_id),  # type: ignore[arg-type]
        params=kw.pop("params", {}),  # type: ignore[arg-type]
        transform=kw.pop("transform", None),  # type: ignore[arg-type]
    )


def _is_valid_python_expr(src: str) -> bool:
    """The emitted `code` should parse on its own, given F as a free name."""
    wrapped = f"F = None\nresult = {src}\n"
    try:
        ast.parse(wrapped)
    except SyntaxError:
        return False
    return True


def test_reformat_emits_select() -> None:
    comp = _make(
        "REFORMAT",
        transform="""
            out.id   :: in.id;
            out.name :: string_upcase(in.name);
        """,
    )
    op = map_component(comp, ["df_in"])
    assert op.var == df_var("c1")
    assert op.code.startswith("df_in.select(")
    assert 'alias("id")' in op.code
    assert "F.upper(" in op.code
    assert _is_valid_python_expr(op.code)


def test_reformat_requires_transform() -> None:
    with pytest.raises(MappingError):
        map_component(_make("REFORMAT"), ["df_in"])


def test_filter_emits_filter() -> None:
    comp = _make(
        "FILTER_BY_EXPRESSION",
        params={"select_expr": 'in.status = "active" and in.amount > 100'},
    )
    op = map_component(comp, ["df_src"])
    assert op.code.startswith("df_src.filter(")
    assert " & " in op.code
    assert _is_valid_python_expr(op.code)


def test_join_inner_default() -> None:
    comp = _make("JOIN", params={"key": "user_id"})
    op = map_component(comp, ["df_left", "df_right"])
    assert op.code == 'df_left.join(df_right, on=["user_id"], how="inner")'


def test_join_supports_outer_aliases() -> None:
    comp = _make("JOIN", params={"key": "id1, id2", "join_type": "left_outer"})
    op = map_component(comp, ["a", "b"])
    assert 'on=["id1", "id2"]' in op.code
    assert 'how="left"' in op.code


def test_join_rejects_three_inputs() -> None:
    comp = _make("JOIN", params={"key": "id"})
    with pytest.raises(MappingError):
        map_component(comp, ["a", "b", "c"])


def test_sort_with_directions() -> None:
    comp = _make("SORT", params={"key": "id asc, score desc"})
    op = map_component(comp, ["df_in"])
    assert op.code == 'df_in.orderBy(F.col("id").asc(), F.col("score").desc())'


def test_dedup_sorted() -> None:
    comp = _make("DEDUP_SORTED", params={"key": "user_id, day"})
    op = map_component(comp, ["df_in"])
    assert op.code == 'df_in.dropDuplicates(["user_id", "day"])'


def test_rollup_groupby_agg() -> None:
    comp = _make(
        "ROLLUP",
        params={"key": "region"},
        transform="""
            out.total :: sum(in.amount);
            out.cnt   :: count_recs();
        """,
    )
    op = map_component(comp, ["df_in"])
    assert op.code.startswith('df_in.groupBy("region")')
    assert ".agg(" in op.code
    assert "F.sum(" in op.code
    assert "F.count('*')" in op.code
    assert _is_valid_python_expr(op.code)


def test_unknown_component_raises() -> None:
    with pytest.raises(MappingError):
        map_component(_make("NORMALIZE"), ["df_in"])
