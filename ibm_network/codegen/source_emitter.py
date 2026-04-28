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
) -> str:
    """Return the Python expression that loads the source DataFrame.

    Args:
        strategy:    chosen by `read_strategy.choose(record)`.
        schema_var:  name of a `StructType` variable to pass as `schema=...`,
                     or None if the strategy doesn't use a schema.
        input_path:  filesystem path to the input file.

    Raises:
        NotImplementedError: for strategies that Phase 3 hasn't filled in yet.
    """
    if strategy is ReadStrategy.CSV_DELIMITED:
        if schema_var is None:
            raise ValueError("CSV_DELIMITED requires a schema_var")
        return f'spark.read.csv("{input_path}", header=True, schema={schema_var})'
    if strategy is ReadStrategy.CSV_INFER:
        return f'spark.read.csv("{input_path}", header=True, inferSchema=True)'
    raise NotImplementedError(f"read strategy not implemented yet: {strategy.value}")
