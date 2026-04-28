"""Render a DML AST as PySpark `StructType(...)` source code.

This module is the **only** place that produces source-string output for the
schema layer. It does not import PySpark — generated code references the names
`StructType`, `StructField`, etc. by string, and the generated `.py` file
imports them itself.

The typed counterpart is `ibm_network.dml.type_mapper.to_struct`. The two paths
must agree byte-for-byte on what types they produce; tests pin both.
"""

from __future__ import annotations

from ibm_network.dml.ast import DmlField, DmlNested, DmlRecord, DmlVoid
from ibm_network.dml.parser import parse_dml
from ibm_network.dml.type_map import spark_type_source


def render_schema(record: DmlRecord, *, indent: int = 4) -> str:
    """Render a `DmlRecord` as a `StructType([...])` source string.

    ``void`` fields are skipped — they exist for positional alignment in the
    source data and are not part of the output schema.
    """
    pad = " " * indent
    lines = ["StructType(["]
    for f in record.fields:
        if isinstance(f.type, DmlVoid):
            continue
        lines.append(f"{pad}{_render_struct_field(f)},")
    lines.append("])")
    return "\n".join(lines)


def render_schema_from_text(text: str, *, indent: int = 4) -> str:
    """Parse `text` and render the resulting record as PySpark schema source."""
    return render_schema(parse_dml(text), indent=indent)


def _render_struct_field(field: DmlField) -> str:
    if isinstance(field.type, DmlNested):
        inner_fields = ", ".join(_render_struct_field(c) for c in field.type.fields)
        type_src = f"StructType([{inner_fields}])"
    else:
        type_src = spark_type_source(field.type)
    if field.vector_length is not None:
        type_src = f"ArrayType({type_src})"
    return f'StructField("{field.name}", {type_src}, True)'
