"""Emit the read code for a source component, given a chosen `ReadStrategy`.

This module is the *only* place that produces the text on the right-hand side
of `df_<id> = ...` for input components. Phase 3 will fill in the strategies
that currently raise `NotImplementedError`.
"""

from __future__ import annotations

from ibm_network.codegen.read_strategy import ReadStrategy


def render_source_read(
    strategy: ReadStrategy,
    *,
    schema_var: str | None,
    input_path: str,
    delimiter: str | None = None,
) -> str:
    """Return the Python expression that loads the source DataFrame.

    Args:
        strategy:    chosen by `read_strategy.choose(record)`.
        schema_var:  name of a `StructType` variable to pass as `schema=...`,
                     or None if the strategy doesn't use a schema.
        input_path:  filesystem path to the input file.
        delimiter:   field delimiter for delimited reads. ``None`` falls back to
                     Spark's default (``,``). Newline (``\\n``) is the row
                     terminator and ignored here.

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
    raise NotImplementedError(f"read strategy not implemented yet: {strategy.value}")


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
