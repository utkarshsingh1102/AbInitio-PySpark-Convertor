"""Map DML scalar AST nodes to PySpark `pyspark.sql.types` source expressions.

We emit *source code strings* rather than constructing Spark type objects directly so the
convertor can run without a PySpark install — the generated `.py` file imports them itself.
"""

from __future__ import annotations

from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlInteger,
    DmlReal,
    DmlScalar,
    DmlString,
)


def spark_type_source(scalar: DmlScalar) -> str:
    if isinstance(scalar, DmlDecimal):
        if scalar.scale == 0:
            return "LongType()"
        precision = scalar.precision if scalar.precision is not None else 38
        return f"DecimalType({precision}, {scalar.scale})"
    if isinstance(scalar, DmlInteger):
        return _integer_source(scalar.size_bytes)
    if isinstance(scalar, DmlReal):
        return "FloatType()" if scalar.size_bytes <= 4 else "DoubleType()"
    if isinstance(scalar, DmlString):
        return "StringType()"
    if isinstance(scalar, DmlDate):
        return "DateType()"
    if isinstance(scalar, DmlDatetime):
        return "TimestampType()"
    raise TypeError(f"unhandled DML scalar: {scalar!r}")


def _integer_source(size_bytes: int) -> str:
    if size_bytes <= 1:
        return "ByteType()"
    if size_bytes == 2:
        return "ShortType()"
    if size_bytes <= 4:
        return "IntegerType()"
    return "LongType()"
