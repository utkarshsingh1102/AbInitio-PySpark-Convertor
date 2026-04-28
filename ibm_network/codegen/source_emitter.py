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
    DmlNested,
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
    elif strategy is ReadStrategy.TEXT_SPLIT_REGEX:
        if record is None:
            raise ValueError("TEXT_SPLIT_REGEX requires a parsed DmlRecord")
        base = _render_text_split_regex(record, input_path)
    elif strategy is ReadStrategy.CSV_MIXED_DELIM:
        if record is None:
            raise ValueError("CSV_MIXED_DELIM requires a parsed DmlRecord")
        base = _render_csv_mixed_delim(record, input_path)
    else:
        raise NotImplementedError(f"read strategy not implemented yet: {strategy.value}")

    if record is not None:
        base += _render_null_replacements(record)
        base += _render_temporal_conversions(record)
        base += _render_vector_assemblies(record)
        base += _render_struct_assemblies(record)
        base += _render_defaults(record)
    return base


def _has_nested(record: DmlRecord) -> bool:
    return any(isinstance(f.type, DmlNested) for f in record.fields)


def _render_struct_assemblies(record: DmlRecord) -> str:
    """Bottom-up roll up flat read columns into nested struct columns (TC-013 /
    TC-014). For each nested field, emits

        .withColumn(name, F.struct(<inner cols>)).drop(<inner cols>)

    after recursing into deeper nesting first. Finally appends a `.select(...)`
    to enforce the top-level field order from the DML.
    """
    if not _has_nested(record):
        return ""
    parts: list[str] = []
    _emit_assemblies(record.fields, parts)
    final_cols = ", ".join(
        f'"{f.name}"' for f in record.fields if not isinstance(f.type, DmlVoid)
    )
    parts.append(f".select({final_cols})")
    return "".join(parts)


def _emit_assemblies(fields, parts: list[str]) -> None:
    for f in fields:
        if isinstance(f.type, DmlNested):
            _emit_assemblies(f.type.fields, parts)
            inner = ", ".join(f'"{c.name}"' for c in f.type.fields)
            parts.append(f'.withColumn("{f.name}", F.struct({inner}))')
            parts.append(f".drop({inner})")


def _needs_inline_schema(record: DmlRecord) -> bool:
    """The CSV_DELIMITED path must use an inline read schema (rather than the
    pre-rendered output ``schema_var``) when the record has

      - voids (read for alignment, then dropped); or
      - date/datetime fields (read as String, then to_date/to_timestamp); or
      - fixed-length vector fields (read as N flat columns, then F.array assembled).
    """
    for f in record.fields:
        if isinstance(f.type, (DmlVoid, DmlDate, DmlDatetime, DmlNested)):
            return True
        if f.vector_length is not None:
            return True
    return False


def _render_vector_assemblies(record: DmlRecord) -> str:
    """For every fixed-length vector field, append:

        .withColumn(name, F.array("name_0", ..., "name_{N-1}"))
        .drop("name_0", ..., "name_{N-1}")

    `_render_csv_with_inline_schema` flattens vectors into N positional columns
    in the read schema; this step rolls them back up into a single ArrayType
    column.
    """
    parts: list[str] = []
    for f in record.fields:
        if not isinstance(f.vector_length, int):
            continue  # variable-length vectors (TC-015) handled separately
        cols = [f'"{f.name}_{j}"' for j in range(f.vector_length)]
        parts.append(f'.withColumn("{f.name}", F.array({", ".join(cols)}))')
        parts.append(f'.drop({", ".join(cols)})')
    return "".join(parts)


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
        if sentinel is None or isinstance(f.type, (DmlVoid, DmlNested)):
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
    _emit_inline_schema(record.fields, inline, void_id=[0])
    schema_inline = "StructType([" + ", ".join(inline) + "])"
    has_voids = any(isinstance(f.type, DmlVoid) for f in record.fields)
    base = f'spark.read{opts}.csv("{input_path}", schema={schema_inline})'
    if has_voids:
        keep = [
            f'"{f.name}_{j}"' if isinstance(f.vector_length, int) else f'"{f.name}"'
            for f in record.fields if not isinstance(f.type, DmlVoid)
            for j in (range(f.vector_length) if isinstance(f.vector_length, int) else [None])
        ]
        base = f'{base}.select({", ".join(keep)})'
    return base


def _emit_inline_schema(fields, inline: list[str], *, void_id: list[int]) -> None:
    """Walk fields (recursing into nested sub-records) and append flat
    `StructField(...)` source strings to `inline`. `void_id` is a single-element
    mutable counter used to generate unique placeholder names for voids.
    """
    for f in fields:
        t = f.type
        if isinstance(t, DmlNested):
            _emit_inline_schema(t.fields, inline, void_id=void_id)
            continue
        if isinstance(t, DmlVoid):
            inline.append(f'StructField("_void_{void_id[0]}", StringType(), True)')
            void_id[0] += 1
            continue
        if isinstance(f.vector_length, int):
            elem_src = spark_type_source(t)
            for j in range(f.vector_length):
                inline.append(f'StructField("{f.name}_{j}", {elem_src}, True)')
            continue
        if isinstance(t, (DmlDate, DmlDatetime)):
            inline.append(f'StructField("{f.name}", StringType(), True)')
        else:
            inline.append(f'StructField("{f.name}", {spark_type_source(t)}, True)')


def _render_csv_mixed_delim(record: DmlRecord, input_path: str) -> str:
    """Read a record with mixed per-field delimiters (TC-010): read raw lines,
    split by a regex character class containing all distinct delimiters, then
    project each field by positional index.

    Casts and date/timestamp parsing are applied inline so the standard
    null/temporal/default chain remains a no-op for already-typed columns.
    """
    delims = sorted({
        d for d in (getattr(f.type, "delimiter", None) for f in record.fields)
        if d and d != "\\n"
    })
    regex_class = "[" + "".join(_regex_escape_class(d) for d in delims) + "]"

    parts: list[str] = []
    idx = 0
    for f in record.fields:
        if isinstance(f.type, DmlVoid):
            idx += 1
            continue
        col = f'F.col("_p").getItem({idx})'
        parts.append(_apply_inline_cast(col, f))
        idx += 1
    body = ",\n            ".join(parts)
    # Outer parens turn the multi-line chain into a single expression so the
    # post-read .withColumn(...) chain can be appended without breaking syntax.
    return (
        f'(spark.read.text("{input_path}")\n'
        f'            .select(F.split(F.col("value"), r"{regex_class}").alias("_p"))\n'
        f'            .select(\n'
        f'                {body},\n'
        f'            ))'
    )


def _apply_inline_cast(col_expr: str, field) -> str:  # type: ignore[no-untyped-def]
    """Cast / parse `col_expr` (a string Column) according to `field.type` and
    alias it to `field.name`. Used by CSV_MIXED_DELIM."""
    t = field.type
    if isinstance(t, DmlDate):
        spark_fmt = to_spark_format(t.format)
        return f'F.to_date({col_expr}, "{spark_fmt}").alias("{field.name}")'
    if isinstance(t, DmlDatetime):
        spark_fmt = to_spark_format(t.format)
        return f'F.to_timestamp({col_expr}, "{spark_fmt}").alias("{field.name}")'
    if isinstance(t, DmlString):
        return f'{col_expr}.alias("{field.name}")'
    cast = _sql_cast(t)
    if cast is None:
        return f'{col_expr}.alias("{field.name}")'
    return f'{col_expr}.cast("{cast}").alias("{field.name}")'


def _render_text_split_regex(record: DmlRecord, input_path: str) -> str:
    """Read a record with one or more variable-length vectors (TC-015): read raw
    lines, split into a string array `_p`, then project each scalar via
    `element_at` and each vector via `slice` (with `transform(...)` casts when
    the element type isn't a string). Offsets accumulate symbolically because
    they depend on runtime discriminator columns.
    """
    delim = None
    for f in record.fields:
        d = getattr(f.type, "delimiter", None)
        if d and d != "\\n":
            delim = d
            break
    delim = delim or ","

    parts: list[str] = []
    constant = 1  # 1-based SQL slice/element_at offset
    sym_terms: list[str] = []

    for f in record.fields:
        t = f.type
        if isinstance(f.vector_length, str):
            disc = f.vector_length
            cur = _format_offset_expr(constant, sym_terms)
            assert not isinstance(t, DmlNested)
            cast = _sql_cast(t) if not isinstance(t, DmlString) else None
            slice_e = f"slice(_p, {cur}, {disc})"
            sql = (
                f"transform({slice_e}, x -> cast(x as {cast}))" if cast else slice_e
            )
            parts.append(f'.withColumn("{f.name}", F.expr("{sql}"))')
            sym_terms.append(disc)
            continue
        # scalar at offset — element_at requires INT, force the cast since
        # offsets that mix in BIGINT discriminator columns otherwise fail.
        assert not isinstance(t, DmlNested)
        cur = _format_offset_expr(constant, sym_terms)
        idx = cur if not sym_terms else f"int({cur})"
        cast = _sql_cast(t)
        e = f"element_at(_p, {idx})"
        sql = f"cast({e} as {cast})" if cast else e
        parts.append(f'.withColumn("{f.name}", F.expr("{sql}"))')
        constant += 1

    parts.append('.drop("_p")')
    body = "".join(parts)
    return (
        f'(spark.read.text("{input_path}")\n'
        f'            .select(F.split(F.col("value"), r"{_regex_escape_class(delim)}").alias("_p"))'
        f"{body})"
    )


def _format_offset_expr(constant: int, sym_terms: list[str]) -> str:
    counts: dict[str, int] = {}
    for s in sym_terms:
        counts[s] = counts.get(s, 0) + 1
    parts = [str(constant)]
    for name, n in counts.items():
        parts.append(name if n == 1 else f"{n}*{name}")
    return " + ".join(parts) if len(parts) > 1 else parts[0]


def _regex_escape_class(d: str) -> str:
    """Escape `d` for safe use inside a regex character class.

    Inside ``[...]`` the special characters are ``\\``, ``]``, ``^`` (only at
    the start), and ``-`` (only between two chars). Escaping all of them is
    always safe.
    """
    if d in ("\\", "]", "^", "-"):
        return "\\" + d
    return d


def _render_text_substring(record: DmlRecord, input_path: str) -> str:
    """Read a fixed-width record by reading raw lines and projecting via substring.

    Walks the record once, accumulating the running 1-based byte offset. Each
    non-void field becomes ``F.substring("value", offset, width).cast(...).alias(...)``.
    Void fields advance the offset but are not projected.
    """
    parts: list[str] = []
    offset = 1
    for f in record.fields:
        assert not isinstance(f.type, DmlNested)
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
