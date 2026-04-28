"""Render a DML AST as PySpark `StructType(...)` source code.

This module is the **only** place that produces source-string output for the
schema layer. It does not import PySpark — generated code references the names
`StructType`, `StructField`, etc. by string, and the generated `.py` file
imports them itself.

The typed counterpart is `ibm_network.dml.type_mapper.to_struct`.
"""

from __future__ import annotations

import json

from ibm_network.dml.ast import DmlField, DmlRecord
from ibm_network.dml.parser import parse_dml
from ibm_network.dml.type_map import field_metadata, spark_type_source


def render_schema(record: DmlRecord, *, indent: int = 4) -> str:
    """Render a `DmlRecord` as a `StructType([...])` source string."""
    pad = " " * indent
    lines = ["StructType(["]
    for f in record.fields:
        lines.append(f"{pad}{_render_struct_field(f)},")
    lines.append("])")
    return "\n".join(lines)


def render_schema_from_text(text: str, *, indent: int = 4) -> str:
    """Parse `text` and render the resulting record as PySpark schema source."""
    return render_schema(parse_dml(text), indent=indent)


def _render_struct_field(field: DmlField) -> str:
    type_src = spark_type_source(field.type)
    meta = field_metadata(field.type)
    meta_arg = f", metadata={json.dumps(meta)}" if meta else ""
    return f'StructField("{field.name}", {type_src}, True{meta_arg})'
