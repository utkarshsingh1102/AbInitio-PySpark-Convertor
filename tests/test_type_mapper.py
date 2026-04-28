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


def test_decimal_default_precision() -> None:
    assert to_dtype(DmlDecimal(delimiter=",")) == DecimalType(38, 0)


def test_integer_size_dispatch() -> None:
    assert to_dtype(DmlInteger(size_bytes=1)) == ByteType()
    assert to_dtype(DmlInteger(size_bytes=2)) == ShortType()
    assert to_dtype(DmlInteger(size_bytes=4)) == IntegerType()
    assert to_dtype(DmlInteger(size_bytes=8)) == LongType()


def test_simple_scalars() -> None:
    assert to_dtype(DmlString(length=20)) == StringType()
    assert to_dtype(DmlDate(format="YYYY-MM-DD")) == DateType()
    assert to_dtype(DmlDatetime(format="YYYYMMDDHHMMSS")) == TimestampType()


def test_to_struct_preserves_metadata() -> None:
    record = DmlRecord(fields=(
        DmlField(name="id", type=DmlDecimal(precision=10, scale=0)),
        DmlField(name="name", type=DmlString(length=20)),
        DmlField(name="signup", type=DmlDate(format="YYYY-MM-DD")),
    ))
    actual = to_struct(record)
    assert actual == StructType([
        StructField("id", DecimalType(10, 0), True),
        StructField("name", StringType(), True, {"length": 20}),
        StructField("signup", DateType(), True, {"format": "YYYY-MM-DD"}),
    ])
