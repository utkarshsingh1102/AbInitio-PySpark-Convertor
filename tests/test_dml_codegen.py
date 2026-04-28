import ast
from pathlib import Path

from ibm_network.dml.emitter import render_schema as schema_to_struct_source
from ibm_network.dml.emitter import render_schema_from_text as dml_text_to_struct_source
from ibm_network.dml.parser import parse_dml_file

FIX = Path(__file__).parent / "fixtures" / "dml"


def _wrap(struct_src: str) -> str:
    """Wrap StructType source so it parses on its own (the real generator imports the names)."""
    return (
        "from pyspark.sql.types import StructType, StructField, "
        "DecimalType, IntegerType, LongType, ShortType, ByteType, "
        "StringType, DateType, TimestampType\n"
        f"schema = {struct_src}\n"
    )


def test_customer_struct_source_parses_as_python() -> None:
    record = parse_dml_file(FIX / "customer.dml")
    src = schema_to_struct_source(record)
    parsed = ast.parse(_wrap(src))
    assert any(
        isinstance(n, ast.Assign) and n.targets[0].id == "schema"  # type: ignore[attr-defined]
        for n in parsed.body
    )


def test_customer_struct_source_contains_expected_types() -> None:
    """`decimal(N)` (no scale) maps to LongType per the suite contract — only
    `decimal("P.S", delim)` produces a true DecimalType."""
    record = parse_dml_file(FIX / "customer.dml")
    src = schema_to_struct_source(record)
    assert 'StructField("customer_id", LongType()' in src
    assert "DecimalType(12, 2)" in src
    assert 'StructField("name", StringType()' in src
    assert "DateType()" in src
    assert "TimestampType()" in src
    assert "IntegerType()" in src


def test_struct_fields_have_no_metadata() -> None:
    """Schema emission no longer leaks DML format strings into StructField metadata —
    the suite expects empty metadata."""
    record = parse_dml_file(FIX / "customer.dml")
    src = schema_to_struct_source(record)
    assert "metadata=" not in src


def test_dml_text_to_struct_source_roundtrip() -> None:
    text = "record decimal(8) id; string(10) name; end"
    src = dml_text_to_struct_source(text)
    assert 'StructField("id", LongType()' in src
    assert 'StructField("name", StringType()' in src


def test_integer_size_dispatch() -> None:
    src = dml_text_to_struct_source(
        "record integer(1) tiny; integer(2) small; integer(4) med; integer(8) big; end"
    )
    assert "ByteType()" in src
    assert "ShortType()" in src
    assert "IntegerType()" in src
    assert "LongType()" in src
