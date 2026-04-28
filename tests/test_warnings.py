from ibm_network.dml.warnings import (
    DmlWarning,
    WarningKind,
    dml_parse_fallback,
    no_dml_available,
    unmappable_component,
)


def test_fallback_kinds_have_no_manual_review_prefix() -> None:
    w = dml_parse_fallback("src", "boom")
    formatted = w.format()
    assert "MANUAL_REVIEW" not in formatted
    assert "[DML_PARSE_FALLBACK]" in formatted
    assert "src" in formatted
    assert "boom" in formatted
    assert not w.is_manual_review()


def test_manual_review_kinds_have_prefix() -> None:
    w = DmlWarning(WarningKind.UNION_DEGRADED, "value", "union → struct of nullables")
    formatted = w.format()
    assert formatted.startswith("MANUAL_REVIEW ")
    assert "[UNION_DEGRADED]" in formatted
    assert w.is_manual_review()


def test_no_dml_available_helper() -> None:
    w = no_dml_available("src1")
    assert w.kind is WarningKind.NO_DML_AVAILABLE
    assert "src1" in w.format()


def test_unmappable_component_helper() -> None:
    w = unmappable_component("c1", "NORMALIZE", "no rule")
    assert w.kind is WarningKind.UNMAPPABLE_COMPONENT
    assert "NORMALIZE" in w.detail
    assert "no rule" in w.detail


def test_warning_is_hashable_and_frozen() -> None:
    w = DmlWarning(WarningKind.PACKED_DECIMAL_MANUAL, "f", "x")
    s = {w, DmlWarning(WarningKind.PACKED_DECIMAL_MANUAL, "f", "x")}
    assert len(s) == 1
