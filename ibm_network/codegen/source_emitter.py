"""Emit the read code for a source component, given a chosen `ReadStrategy`.

This module is the *only* place that produces the text on the right-hand side
of `df_<id> = ...` for input components. Phase 3 will fill in the strategies
that currently raise `NotImplementedError`.
"""

from __future__ import annotations

from ibm_network.codegen.read_strategy import ReadStrategy
from ibm_network.dml.ast import (
    DmlCondition,
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlField,
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


def render_raw_schema_source(record: DmlRecord) -> str | None:
    """Return a ``StructType([...])`` source string for the *read-time* schema,
    or ``None`` when the read schema equals the output schema.

    When ``_needs_inline_schema`` is true the CSV reader uses a schema that
    differs from the final output schema (sentinel-numeric fields as StringType,
    date/datetime as StringType, void placeholder columns, etc.).  Callers
    should assign the returned source to a ``raw_schema`` variable and pass
    ``raw_schema_var="raw_schema"`` to ``render_source_read``.
    """
    if not _needs_inline_schema(record):
        return None
    inline: list[str] = []
    _emit_inline_schema(record.fields, inline, void_id=[0])
    rows = ",\n    ".join(inline)
    return f"StructType([\n    {rows},\n])"


def render_source_read(
    strategy: ReadStrategy,
    *,
    schema_var: str | None,
    input_path: str,
    delimiter: str | None = None,
    record: DmlRecord | None = None,
    raw_schema_var: str = "raw_schema",
) -> str:
    """Return the Python expression that loads the source DataFrame.

    Args:
        strategy:       chosen by `read_strategy.choose(record)`.
        schema_var:     name of the final ``StructType`` variable, or None.
        input_path:     filesystem path to the input file.
        delimiter:      field delimiter for delimited reads.
        record:         parsed DML; required for substring/binary strategies.
        raw_schema_var: name of the read-time schema variable emitted by
                        ``render_raw_schema_source``; used when the read
                        schema diverges from the output schema.

    Raises:
        NotImplementedError: for strategies that Phase 3 hasn't filled in yet.
    """
    if strategy is ReadStrategy.CSV_DELIMITED:
        if schema_var is None:
            raise ValueError("CSV_DELIMITED requires a schema_var")
        if record is not None and _needs_inline_schema(record):
            base = _render_csv_with_inline_schema(
                record, input_path, delimiter, raw_schema_var
            )
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
        base += _render_conditional_projections(record)
        base += _render_defaults(record)
    return base


def _iter_leaves(fields):
    """Yield non-Nested DmlFields, descending through DmlNested containers so
    helpers (defaults, null replacement, temporal conversion) can apply by leaf
    name regardless of how deep the field sits in a sub-record / union.

    Conditional branches (TC-017 / TC-018) are skipped — their leaves never
    exist as flat columns at read time; the conditional projection casts them
    inline.
    """
    for f in fields:
        if f.condition is not None or f.is_else:
            continue
        if isinstance(f.type, DmlNested):
            yield from _iter_leaves(f.type.fields)
        else:
            yield f


def _has_nested(record: DmlRecord) -> bool:
    return any(isinstance(f.type, DmlNested) for f in record.fields)


def _render_struct_assemblies(record: DmlRecord) -> str:
    """Bottom-up roll up flat read columns into nested struct columns (TC-013 /
    TC-014). For each nested field, emits

        .withColumn(name, F.struct(<inner cols>)).drop(<inner cols>)

    after recursing into deeper nesting first. Appends a `.select(...)` only
    when no conditional fields are present; otherwise `_render_conditional_projections`
    owns the final select.
    """
    if not _has_nested(record):
        return ""
    parts: list[str] = []
    _emit_assemblies(record.fields, parts)
    has_conds = any(f.condition is not None or f.is_else for f in record.fields)
    if not has_conds:
        final_cols = ", ".join(
            f'"{f.name}"' for f in record.fields if not isinstance(f.type, DmlVoid)
        )
        parts.append(f".select({final_cols})")
    return "".join(parts)


def _emit_assemblies(fields, parts: list[str]) -> None:
    for f in fields:
        if f.condition is not None or f.is_else:
            # Conditional fields are assembled by _render_conditional_projections.
            continue
        if isinstance(f.type, DmlNested):
            # Vector-of-records fields are assembled in-place by the
            # TEXT_SPLIT_REGEX path; their inner column names never exist as
            # flat columns, so don't try to F.struct(...) them.
            if f.vector_length is not None:
                continue
            _emit_assemblies(f.type.fields, parts)
            inner = ", ".join(f'"{c.name}"' for c in f.type.fields)
            parts.append(f'.withColumn("{f.name}", F.struct({inner}))')
            parts.append(f".drop({inner})")


def _needs_inline_schema(record: DmlRecord) -> bool:
    """The CSV_DELIMITED path must use an inline read schema (rather than the
    pre-rendered output ``schema_var``) when the record has

      - voids (read for alignment, then dropped); or
      - date/datetime fields (read as String, then to_date/to_timestamp); or
      - fixed-length vector fields (read as N flat columns, then F.array assembled); or
      - numeric fields with a null sentinel (read as String so the raw token is
        compared before parsing — Ab Initio's compare-before-parse semantics).
    """
    for f in record.fields:
        if f.condition is not None or f.is_else:
            return True
        if isinstance(f.type, (DmlVoid, DmlDate, DmlDatetime, DmlNested)):
            return True
        if f.vector_length is not None:
            return True
        if _is_numeric_with_sentinel(f.type):
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


def _render_conditional_projections(record: DmlRecord) -> str:
    """For each run of consecutive conditional fields (TC-017 / TC-018), emit:

      - One `.withColumn(branch_name, F.when(activation, value_expr))` per branch.
      - `.drop("_cN_0", "_cN_1", ...)` to remove the placeholder slots.

    Ends with a `.select(...)` that enforces the final output column order.
    """
    has_conds = any(f.condition is not None or f.is_else for f in record.fields)
    if not has_conds:
        return ""

    parts: list[str] = []
    run_id = 0
    i = 0
    fields = record.fields

    while i < len(fields):
        f = fields[i]
        if f.condition is None and not f.is_else:
            i += 1
            continue

        j = i
        run: list[DmlField] = []
        while j < len(fields) and (fields[j].condition is not None or fields[j].is_else):
            run.append(fields[j])
            j += 1

        width = max(_branch_slot_width(rf) for rf in run)
        slot_cols = [f"_c{run_id}_{k}" for k in range(width)]

        for branch in run:
            act = _activation_expr(branch)
            if isinstance(branch.type, DmlNested):
                inner_parts: list[str] = []
                for k, inner in enumerate(branch.type.fields):
                    slot = f'F.col("_c{run_id}_{k}")'
                    cast = _sql_cast(inner.type)
                    if cast:
                        slot = f'{slot}.cast("{cast}")'
                    inner_parts.append(f'{slot}.alias("{inner.name}")')
                struct_expr = f'F.struct({", ".join(inner_parts)})'
                expr = f'F.when({act}, {struct_expr})'
            else:
                slot = f'F.col("_c{run_id}_0")'
                cast = _sql_cast(branch.type)
                if cast:
                    slot = f'{slot}.cast("{cast}")'
                expr = f'F.when({act}, {slot})'
            parts.append(f'.withColumn("{branch.name}", {expr})')

        drop_cols = ", ".join(f'"{c}"' for c in slot_cols)
        parts.append(f'.drop({drop_cols})')

        run_id += 1
        i = j

    all_output_cols = ", ".join(
        f'"{f.name}"' for f in record.fields if not isinstance(f.type, DmlVoid)
    )
    parts.append(f".select({all_output_cols})")
    return "".join(parts)


def _activation_expr(field: DmlField) -> str:
    if field.is_else:
        neg_parts = [
            f'(F.col("{c.column}") != F.lit({_cond_val_lit(c.value)}))'
            for c in field.excludes
        ]
        return " & ".join(neg_parts) if neg_parts else "F.lit(True)"
    c = field.condition
    assert c is not None
    return f'F.col("{c.column}") == F.lit({_cond_val_lit(c.value)})'


def _cond_val_lit(value: str | int | float) -> str:
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def _render_defaults(record: DmlRecord) -> str:
    """Append a single ``.fillna({col: default, ...})`` for fields that declare
    a default value (``decimal(",") qty = 0;``). Spark's fillna ignores keys
    whose column type doesn't match the default's type, so each column is
    safe to include in one dict.
    """
    pairs: list[str] = []
    for f in _iter_leaves(record.fields):
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
    for f in _iter_leaves(record.fields):
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


def _is_numeric_with_sentinel(scalar) -> bool:  # type: ignore[no-untyped-def]
    """True when a field is a numeric type AND carries a non-empty null sentinel.

    These fields must be read as StringType so that the raw token is compared
    against the sentinel string before the value is parsed to a number — matching
    Ab Initio's compare-before-parse semantics.
    """
    return (
        isinstance(scalar, (DmlInteger, DmlReal, DmlDecimal))
        and bool(getattr(scalar, "null_value", None))
    )


def _render_null_replacements(record: DmlRecord) -> str:
    """For every field whose DML carries a ``null("...")`` indicator, append a
    ``.withColumn(name, F.when(col == lit(sentinel), None).otherwise(col))``
    step.  For numeric fields the column is still StringType at this point
    (read as string so the raw token is compared); the cast to the target
    numeric type is inlined into the ``.otherwise(...)`` expression so a
    single ``.withColumn(...)`` both nullifies the sentinel and restores the
    correct type.
    """
    parts: list[str] = []
    for f in _iter_leaves(record.fields):
        sentinel = getattr(f.type, "null_value", None)
        if sentinel is None or isinstance(f.type, (DmlVoid, DmlNested)):
            continue
        lit = _format_null_lit(sentinel, f.type)
        if lit is None:
            continue
        if _is_numeric_with_sentinel(f.type):
            cast = _sql_cast(f.type)
            otherwise = f'F.col("{f.name}").cast("{cast}")'
        else:
            otherwise = f'F.col("{f.name}")'
        parts.append(
            f'.withColumn("{f.name}", '
            f'F.when(F.col("{f.name}") == {lit}, None)'
            f'.otherwise({otherwise}))'
        )
    return "".join(parts)


def _format_null_lit(sentinel: str, scalar: DmlScalar) -> str | None:
    """Render a null sentinel as a PySpark ``F.lit(...)`` string expression.

    Numeric fields with a non-empty sentinel are read as StringType by
    ``_emit_inline_schema`` (compare-before-parse), so the comparison is
    always string-vs-string.  An empty-string sentinel on a numeric field
    returns ``None`` — Spark's CSV reader already maps empty cells to null
    for typed columns, so no explicit step is needed.
    """
    if not sentinel and isinstance(scalar, (DmlInteger, DmlReal, DmlDecimal)):
        return None
    return f'F.lit("{sentinel}")'


def _render_csv_with_inline_schema(
    record: DmlRecord, input_path: str, delimiter: str | None,
    raw_schema_var: str = "raw_schema",
) -> str:
    """CSV read where the read-time schema differs from the output schema.

    References ``raw_schema_var`` (a variable name defined in the caller's scope
    by ``render_raw_schema_source``) via ``.schema(...).csv(path)`` so the
    schema definition is not embedded inline in the call chain.

    Appends ``.select(keep)`` to drop positional void placeholder columns when
    the record contains void fields.
    """
    opts = _csv_options(delimiter)
    base = f'spark.read{opts}.schema({raw_schema_var}).csv("{input_path}")'
    has_voids = any(isinstance(f.type, DmlVoid) for f in record.fields)
    if has_voids:
        keep = [
            f'"{f.name}_{j}"' if isinstance(f.vector_length, int) else f'"{f.name}"'
            for f in record.fields if not isinstance(f.type, DmlVoid)
            for j in (range(f.vector_length) if isinstance(f.vector_length, int) else [None])
        ]
        base = f'{base}.select({", ".join(keep)})'
    return base


def _emit_inline_schema(fields, inline: list[str], *, void_id: list[int],
                        run_id: list[int] | None = None) -> None:
    """Walk fields (recursing into nested sub-records) and append flat
    `StructField(...)` source strings to `inline`. `void_id` and `run_id`
    are single-element mutable counters used to generate unique placeholder
    names for voids and conditional-region slots respectively.
    """
    if run_id is None:
        run_id = [0]
    i = 0
    while i < len(fields):
        f = fields[i]
        if f.condition is not None or f.is_else:
            j = i
            run = []
            while j < len(fields) and (
                fields[j].condition is not None or fields[j].is_else
            ):
                run.append(fields[j])
                j += 1
            width = max(_branch_slot_width(rf) for rf in run)
            for k in range(width):
                inline.append(f'StructField("_c{run_id[0]}_{k}", StringType(), True)')
            run_id[0] += 1
            i = j
            continue
        t = f.type
        if isinstance(t, DmlNested):
            _emit_inline_schema(t.fields, inline, void_id=void_id, run_id=run_id)
            i += 1
            continue
        if isinstance(t, DmlVoid):
            inline.append(f'StructField("_void_{void_id[0]}", StringType(), True)')
            void_id[0] += 1
            i += 1
            continue
        if isinstance(f.vector_length, int):
            elem_src = spark_type_source(t)
            for j2 in range(f.vector_length):
                inline.append(f'StructField("{f.name}_{j2}", {elem_src}, True)')
            i += 1
            continue
        if isinstance(t, (DmlDate, DmlDatetime)) or _is_numeric_with_sentinel(t):
            # Keep as StringType so the raw token can be compared against the
            # null sentinel (or parsed with to_date/to_timestamp) post-read.
            inline.append(f'StructField("{f.name}", StringType(), True)')
        else:
            inline.append(f'StructField("{f.name}", {spark_type_source(t)}, True)')
        i += 1


def _branch_slot_width(field: DmlField) -> int:
    """Number of positional slots a single conditional branch occupies."""
    if isinstance(field.type, DmlNested):
        return len(field.type.fields)
    return 1


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
    if _is_numeric_with_sentinel(t):
        # Keep as string so _render_null_replacements can compare the raw token.
        # _render_post_null_casts will cast to the target type afterward.
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
        if isinstance(t, DmlNested) and isinstance(f.vector_length, str):
            disc = f.vector_length
            base = _format_offset_expr(constant, sym_terms)
            n = len(t.fields)
            pairs: list[str] = []
            for j, inner in enumerate(t.fields):
                idx = f"int({base} + i * {n} + {j})"
                e = f"element_at(_p, {idx})"
                cast = _sql_cast(inner.type) if not isinstance(inner.type, DmlString) else None
                pairs.append(f"'{inner.name}', cast({e} as {cast})" if cast else f"'{inner.name}', {e}")
            sql = (
                f"transform(sequence(0, {disc} - 1), "
                f"i -> named_struct({', '.join(pairs)}))"
            )
            parts.append(f'.withColumn("{f.name}", F.expr("{sql}"))')
            sym_terms.extend([disc] * n)
            continue
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
        e = f"element_at(_p, {idx})"
        if _is_numeric_with_sentinel(t):
            # Keep as string; _render_null_replacements compares the raw token,
            # then _render_post_null_casts casts to the target type.
            sql = e
        else:
            cast = _sql_cast(t)
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
            if _is_numeric_with_sentinel(f.type):
                # Fixed-width fields are space-padded; trim before the null
                # comparison so "-1   " matches sentinel "-1".  The cast is
                # deferred to _render_post_null_casts after null replacement.
                expr = f'F.trim({expr})'
            elif cast_to is not None:
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


def format_read_chain(expr: str) -> str:
    """Format a Spark read chain with one method per line.

    ``spark.read`` stays on the first line (4-space indent); all subsequent
    method calls are indented 8 spaces.  ``.withColumn(...)`` calls whose
    second argument begins with ``F.when(...)`` are split across three lines::

        (
            spark.read
                .option("header", "false")
                .schema(raw_schema)
                .csv("<path>")
                .withColumn("discount",
                    F.when(F.col("discount") == F.lit("-1"), None)
                     .otherwise(F.col("discount").cast("decimal(10,2)")))
        )
    """
    segments = _split_chain_dots(expr)
    if len(segments) <= 2:
        return expr

    # Keep "spark.read" on one line
    if len(segments) >= 2 and segments[0] == "spark" and segments[1] == "read":
        first = "spark.read"
        rest = segments[2:]
    else:
        first = segments[0]
        rest = segments[1:]

    lines = ["    " + first]
    for seg in rest:
        lines.append(_format_chain_segment(seg))
    return "(\n" + "\n".join(lines) + "\n)"


def _split_chain_dots(expr: str) -> list[str]:
    """Split ``expr`` at every top-level dot (paren/bracket depth == 0)."""
    segments: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(expr):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "." and depth == 0 and i > start:
            segments.append(expr[start:i])
            start = i + 1
    segments.append(expr[start:])
    return segments


def _format_chain_segment(seg: str) -> str:
    """Format a single chain segment at 8-space indent with a leading dot."""
    if seg.startswith("withColumn("):
        return _format_withcolumn(seg)
    return "        ." + seg


def _format_withcolumn(seg: str) -> str:
    """Format ``.withColumn("name", expr)`` — multi-line when expr is F.when(...)."""
    # Strip "withColumn(" prefix and the matching trailing ")"
    inner = seg[len("withColumn("):-1]

    col_name, expr = _split_first_comma(inner)
    expr = expr.strip()

    if "F.when(" not in expr:
        return f"        .withColumn({inner})"

    otherwise_idx = _find_dot_otherwise(expr)
    if otherwise_idx == -1:
        return f"        .withColumn({col_name},\n            {expr})"

    when_part = expr[:otherwise_idx]
    otherwise_part = expr[otherwise_idx:]
    return (
        f"        .withColumn({col_name},\n"
        f"            {when_part}\n"
        f"             {otherwise_part})"
    )


def _split_first_comma(s: str) -> tuple[str, str]:
    """Split ``s`` at the first top-level comma; return (before, after)."""
    depth = 0
    for i, ch in enumerate(s):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            return s[:i], s[i + 1:]
    return s, ""


def _find_dot_otherwise(expr: str) -> int:
    """Return the index of the first top-level ``.otherwise(`` in ``expr``, or -1."""
    depth = 0
    for i, ch in enumerate(expr):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "." and depth == 0 and expr[i:].startswith(".otherwise("):
            return i
    return -1


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
