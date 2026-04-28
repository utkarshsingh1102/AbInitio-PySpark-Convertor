"""Unit tests for ibm_network.dml.type_mapper.

These run against a real pyspark install (the test env has it). Skip cleanly if
pyspark isn't available — the type_mapper module imports it lazily.
"""

from __future__ import annotations

import pytest

from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlField,
    DmlInteger,
    DmlRecord,
    DmlString,
)

pytest.importorskip("pyspark.sql.types")

from pyspark.sql.types import (  # noqa: E402  -- after importorskip
    ByteType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ibm_network.dml.type_mapper import to_dtype, to_struct  # noqa: E402


def test_decimal_with_precision_and_scale() -> None:
    assert to_dtype(DmlDecimal(precision=10, scale=2)) == DecimalType(10, 2)


def test_decimal_no_scale_is_long() -> None:
    """`decimal(N)` and `decimal(",")` map to LongType — only an explicit scale
    > 0 produces a true DecimalType. See DML_TEST_SUITE TC-001..TC-003."""
    assert to_dtype(DmlDecimal(delimiter=",")) == LongType()
    assert to_dtype(DmlDecimal(precision=10, scale=0)) == LongType()


def test_integer_size_dispatch() -> None:
    assert to_dtype(DmlInteger(size_bytes=1)) == ByteType()
    assert to_dtype(DmlInteger(size_bytes=2)) == ShortType()
    assert to_dtype(DmlInteger(size_bytes=4)) == IntegerType()
    assert to_dtype(DmlInteger(size_bytes=8)) == LongType()


def test_simple_scalars() -> None:
    assert to_dtype(DmlString(length=20)) == StringType()
    assert to_dtype(DmlDate(format="YYYY-MM-DD")) == DateType()
    assert to_dtype(DmlDatetime(format="YYYYMMDDHHMMSS")) == TimestampType()


def test_to_struct_omits_metadata() -> None:
    """StructFields are emitted without metadata so the typed schema matches the
    suite's expected_schema.json (which has empty metadata everywhere)."""
    record = DmlRecord(fields=(
        DmlField(name="id", type=DmlDecimal(precision=10, scale=0)),
        DmlField(name="name", type=DmlString(length=20)),
        DmlField(name="signup", type=DmlDate(format="YYYY-MM-DD")),
    ))
    actual = to_struct(record)
    assert actual == StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("signup", DateType(), True),
    ])
