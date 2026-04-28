"""Emit the read code for a source component, given a chosen `ReadStrategy`.

This module is the *only* place that produces the text on the right-hand side
of `df_<id> = ...` for input components. Phase 3 will fill in the strategies
that currently raise `NotImplementedError`.
"""

from __future__ import annotations

from ibm_network.codegen.read_strategy import ReadStrategy
from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlInteger,
    DmlRecord,
    DmlScalar,
    DmlString,
)


def render_source_read(
    strategy: ReadStrategy,
    *,
    schema_var: str | None,
    input_path: str,
    delimiter: str | None = None,
    record: DmlRecord | None = None,
) -> str:
    """Return the Python expression that loads the source DataFrame.

    Args:
        strategy:    chosen by `read_strategy.choose(record)`.
        schema_var:  name of a `StructType` variable to pass as ``schema=...``,
                     or None if the strategy doesn't use a schema.
        input_path:  filesystem path to the input file.
        delimiter:   field delimiter for delimited reads. ``None`` falls back to
                     Spark's default. Newline is the row terminator and ignored.
        record:      parsed DML; required for the substring/binary strategies.

    Raises:
        NotImplementedError: for strategies that Phase 3 hasn't filled in yet.
    """
    if strategy is ReadStrategy.CSV_DELIMITED:
        if schema_var is None:
            raise ValueError("CSV_DELIMITED requires a schema_var")
        opts = _csv_options(delimiter)
        return f'spark.read{opts}.csv("{input_path}", schema={schema_var})'
    if strategy is ReadStrategy.CSV_INFER:
        opts = _csv_options(delimiter)
        return f'spark.read{opts}.csv("{input_path}", inferSchema=True)'
    if strategy is ReadStrategy.TEXT_SUBSTRING:
        if record is None:
            raise ValueError("TEXT_SUBSTRING requires a parsed DmlRecord")
        return _render_text_substring(record, input_path)
    raise NotImplementedError(f"read strategy not implemented yet: {strategy.value}")


def _render_text_substring(record: DmlRecord, input_path: str) -> str:
    """Read a fixed-width record by reading raw lines and projecting via substring.

    Walks the record once, accumulating the running 1-based byte offset. Each
    field becomes ``F.substring("value", offset, width).cast("...").alias("name")``.
    """
    parts: list[str] = []
    offset = 1
    for f in record.fields:
        width = _fixed_width(f.type)
        cast_to = _sql_cast(f.type)
        expr = f'F.substring("value", {offset}, {width})'
        if cast_to is not None:
            expr = f'{expr}.cast("{cast_to}")'
        parts.append(f'{expr}.alias("{f.name}")')
        offset += width
    body = ",\n            ".join(parts)
    return (
        f'spark.read.text("{input_path}").select(\n'
        f'            {body},\n'
        f'        )'
    )


def _fixed_width(scalar: DmlScalar) -> int:
    if isinstance(scalar, DmlString) and scalar.length is not None:
        return scalar.length
    if isinstance(scalar, DmlDecimal) and scalar.precision is not None:
        return scalar.precision
    if isinstance(scalar, DmlInteger):
        return scalar.size_bytes
    raise ValueError(f"cannot derive fixed width for {scalar!r}")


def _sql_cast(scalar: DmlScalar) -> str | None:
    """SQL-cast target for a substring of bytes from a fixed-width record. Returns
    ``None`` for fields that are already strings (no cast needed).
    """
    if isinstance(scalar, DmlString):
        return None
    if isinstance(scalar, DmlDecimal):
        if scalar.scale == 0:
            return "long"
        precision = scalar.precision if scalar.precision is not None else 38
        return f"decimal({precision},{scalar.scale})"
    if isinstance(scalar, DmlInteger):
        if scalar.size_bytes <= 1:
            return "byte"
        if scalar.size_bytes == 2:
            return "short"
        if scalar.size_bytes <= 4:
            return "int"
        return "long"
    if isinstance(scalar, DmlDate):
        return "date"
    if isinstance(scalar, DmlDatetime):
        return "timestamp"
    raise TypeError(f"unhandled DML scalar in cast: {scalar!r}")


def _csv_options(delimiter: str | None) -> str:
    """Build a chain of `.option(...)` calls for a CSV read.

    Ab Initio data files don't carry a CSV header row (the DML provides the
    schema), so we always emit ``header=False``.
    """
    parts = ['.option("header", "false")']
    if delimiter is not None and delimiter != "\n":
        # Escape backslash and double-quote for safe embedding in Python source.
        safe = delimiter.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'.option("delimiter", "{safe}")')
    return "".join(parts)
