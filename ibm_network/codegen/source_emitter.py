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
    DmlReal,
    DmlRecord,
    DmlScalar,
    DmlString,
    DmlVoid,
)
from ibm_network.dml.format_map import to_spark_format
from ibm_network.dml.type_map import spark_type_source


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
        if record is not None and _needs_inline_schema(record):
            base = _render_csv_with_inline_schema(record, input_path, delimiter)
        else:
            opts = _csv_options(delimiter)
            base = f'spark.read{opts}.csv("{input_path}", schema={schema_var})'
    elif strategy is ReadStrategy.CSV_INFER:
        opts = _csv_options(delimiter)
        base = f'spark.read{opts}.csv("{input_path}", inferSchema=True)'
    elif strategy is ReadStrategy.TEXT_SUBSTRING:
        if record is None:
            raise ValueError("TEXT_SUBSTRING requires a parsed DmlRecord")
        base = _render_text_substring(record, input_path)
    else:
        raise NotImplementedError(f"read strategy not implemented yet: {strategy.value}")

    if record is not None:
        base += _render_null_replacements(record)
        base += _render_temporal_conversions(record)
        base += _render_defaults(record)
    return base


def _needs_inline_schema(record: DmlRecord) -> bool:
    """The CSV_DELIMITED path must use an inline read schema (rather than the
    pre-rendered output ``schema_var``) when the record has voids (need to drop)
    or date/datetime fields with custom formats (need to read as String, then
    convert via to_date / to_timestamp because Spark's CSV ``dateFormat`` option
    is per-read, not per-column).
    """
    return any(isinstance(f.type, (DmlVoid, DmlDate, DmlDatetime)) for f in record.fields)


def _render_defaults(record: DmlRecord) -> str:
    """Append a single ``.fillna({col: default, ...})`` for fields that declare
    a default value (``decimal(",") qty = 0;``). Spark's fillna ignores keys
    whose column type doesn't match the default's type, so each column is
    safe to include in one dict.
    """
    pairs: list[str] = []
    for f in record.fields:
        if isinstance(f.type, DmlVoid) or f.default is None:
            continue
        pairs.append(f'"{f.name}": {_default_literal(f.default)}')
    if not pairs:
        return ""
    return ".fillna({" + ", ".join(pairs) + "})"


def _default_literal(value: str | int | float) -> str:
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def _render_temporal_conversions(record: DmlRecord) -> str:
    """For every Date / Datetime field in `record`, append a `.withColumn(...)`
    chain that parses the string column with the field's Spark format pattern.
    Inline-schema reads keep date/datetime columns as StringType so this step
    has something to convert.
    """
    parts: list[str] = []
    for f in record.fields:
        t = f.type
        if isinstance(t, DmlDate):
            spark_fmt = to_spark_format(t.format)
            parts.append(
                f'.withColumn("{f.name}", '
                f'F.to_date(F.col("{f.name}"), "{spark_fmt}"))'
            )
        elif isinstance(t, DmlDatetime):
            spark_fmt = to_spark_format(t.format)
            parts.append(
                f'.withColumn("{f.name}", '
                f'F.to_timestamp(F.col("{f.name}"), "{spark_fmt}"))'
            )
    return "".join(parts)


def _render_null_replacements(record: DmlRecord) -> str:
    """For every field whose DML carries a `null("...")` indicator, append a
    ``.withColumn(name, F.when(col == lit(sentinel), None).otherwise(col))`` step.

    Spark's CSV reader's global ``nullValue`` option only handles one sentinel;
    real Ab Initio records have *per-field* sentinels (TC-006).
    """
    parts: list[str] = []
    for f in record.fields:
        sentinel = getattr(f.type, "null_value", None)
        if sentinel is None or isinstance(f.type, DmlVoid):
            continue
        lit = _format_null_lit(sentinel, f.type)
        if lit is None:
            # Sentinel can't be compared cleanly (e.g., empty-string sentinel on a
            # numeric column). Spark's CSV reader already maps empty cells to null
            # on typed schemas, so no extra step is needed here.
            continue
        parts.append(
            f'.withColumn("{f.name}", '
            f'F.when(F.col("{f.name}") == {lit}, None)'
            f'.otherwise(F.col("{f.name}")))'
        )
    return "".join(parts)


def _format_null_lit(sentinel: str, scalar: DmlScalar) -> str | None:
    """Render a null sentinel as a PySpark `F.lit(...)` expression.

    Returns ``None`` when the sentinel cannot be safely compared against the
    field's type — e.g., an empty-string sentinel on a numeric column would
    force Spark to cast ``""`` to BIGINT and crash. The caller skips that
    step entirely.
    """
    is_numeric = isinstance(scalar, (DmlInteger, DmlReal, DmlDecimal))
    if is_numeric:
        if not sentinel:
            return None
        try:
            float(sentinel)
        except ValueError:
            return None
        return f"F.lit({sentinel})"
    return f'F.lit("{sentinel}")'


def _render_csv_with_inline_schema(
    record: DmlRecord, input_path: str, delimiter: str | None
) -> str:
    """CSV read with an inline schema, used when the output schema diverges from
    the read-time schema:

      - voids are read for positional alignment but selected out;
      - date/datetime fields are read as StringType because Spark CSV's
        ``dateFormat`` option is global per read; later, ``_render_temporal_conversions``
        appends ``.withColumn(...)`` calls that parse them with the right pattern.
    """
    opts = _csv_options(delimiter)
    inline: list[str] = []
    for i, f in enumerate(record.fields):
        t = f.type
        if isinstance(t, DmlVoid):
            inline.append(f'StructField("_void_{i}", StringType(), True)')
        elif isinstance(t, (DmlDate, DmlDatetime)):
            inline.append(f'StructField("{f.name}", StringType(), True)')
        else:
            inline.append(f'StructField("{f.name}", {spark_type_source(t)}, True)')
    schema_inline = "StructType([" + ", ".join(inline) + "])"
    has_voids = any(isinstance(f.type, DmlVoid) for f in record.fields)
    base = f'spark.read{opts}.csv("{input_path}", schema={schema_inline})'
    if has_voids:
        keep = [f'"{f.name}"' for f in record.fields if not isinstance(f.type, DmlVoid)]
        base = f'{base}.select({", ".join(keep)})'
    return base


def _render_text_substring(record: DmlRecord, input_path: str) -> str:
    """Read a fixed-width record by reading raw lines and projecting via substring.

    Walks the record once, accumulating the running 1-based byte offset. Each
    non-void field becomes ``F.substring("value", offset, width).cast(...).alias(...)``.
    Void fields advance the offset but are not projected.
    """
    parts: list[str] = []
    offset = 1
    for f in record.fields:
        width = _fixed_width(f.type)
        if not isinstance(f.type, DmlVoid):
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
    if isinstance(scalar, DmlVoid) and scalar.length is not None:
        return scalar.length
    if isinstance(scalar, DmlDecimal) and scalar.precision is not None:
        return scalar.precision
    if isinstance(scalar, DmlInteger):
        return scalar.size_bytes
    if isinstance(scalar, DmlReal):
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
    if isinstance(scalar, DmlReal):
        return "float" if scalar.size_bytes <= 4 else "double"
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
