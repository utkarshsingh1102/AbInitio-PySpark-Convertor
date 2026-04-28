"""DML type-AST → `pyspark.sql.types.DataType` (pure functions).

This module is the canonical, typed counterpart to the source-string emitter in
`ibm_network.dml.emitter`. Code generation does **not** need PySpark installed,
so the emitter path stays string-based; this module exists for tests, schema
validation, and any caller that wants a real Spark `DataType` for runtime use.

PySpark is imported lazily inside each public function so that importing this
module never forces PySpark to be installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlField,
    DmlInteger,
    DmlReal,
    DmlRecord,
    DmlScalar,
    DmlString,
    DmlVoid,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql.types import DataType, StructField, StructType


# Map integer size in bytes → Spark integer subtype class name. Single source of
# truth; consumed both by `to_dtype` here and by the source-string codegen in
# `type_map._integer_source`. Keep them in sync.
INTEGER_BY_SIZE: dict[int, str] = {1: "ByteType", 2: "ShortType", 4: "IntegerType"}
# Anything larger than 4 bytes maps to LongType.


def to_dtype(scalar: DmlScalar) -> DataType:
    """Convert a DML scalar AST node to a Spark `DataType` instance.

    Raises:
        TypeError: if `scalar` is not a known DML type.
    """
    from pyspark.sql import types as T

    if isinstance(scalar, DmlDecimal):
        # Ab Initio convention: a decimal with no fractional component is a signed
        # integer regardless of byte width. Only `decimal("P.S", delim)` (scale > 0)
        # maps to a true DecimalType.
        if scalar.scale == 0:
            return T.LongType()
        precision = scalar.precision if scalar.precision is not None else 38
        return T.DecimalType(precision, scalar.scale)
    if isinstance(scalar, DmlInteger):
        if scalar.size_bytes <= 1:
            return T.ByteType()
        if scalar.size_bytes == 2:
            return T.ShortType()
        if scalar.size_bytes <= 4:
            return T.IntegerType()
        return T.LongType()
    if isinstance(scalar, DmlReal):
        return T.FloatType() if scalar.size_bytes <= 4 else T.DoubleType()
    if isinstance(scalar, DmlString):
        return T.StringType()
    if isinstance(scalar, DmlDate):
        return T.DateType()
    if isinstance(scalar, DmlDatetime):
        return T.TimestampType()
    raise TypeError(f"unhandled DML scalar: {scalar!r}")


def to_struct_field(field: DmlField) -> StructField:
    """Convert a `DmlField` to a `StructField` (nullable=True, no metadata)."""
    from pyspark.sql.types import StructField

    return StructField(field.name, to_dtype(field.type), True)


def to_struct(record: DmlRecord) -> StructType:
    """Convert a `DmlRecord` to a `StructType`. ``void`` fields are dropped from
    the output schema (see DML_TEST_SUITE TC-005); they exist only to preserve
    positional alignment in the source data.
    """
    from pyspark.sql.types import StructType

    return StructType([to_struct_field(f) for f in record.fields if not isinstance(f.type, DmlVoid)])
