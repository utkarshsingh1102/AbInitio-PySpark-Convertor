"""Render a parsed DML record as a `StructType(...)` source expression."""

from __future__ import annotations

import json

from ibm_network.dml.ast import DmlRecord
from ibm_network.dml.parser import parse_dml
from ibm_network.dml.type_map import field_metadata, spark_type_source


def schema_to_struct_source(record: DmlRecord, *, indent: int = 4) -> str:
    pad = " " * indent
    lines = ["StructType(["]
    for f in record.fields:
        type_src = spark_type_source(f.type)
        meta = field_metadata(f.type)
        meta_arg = f", metadata={json.dumps(meta)}" if meta else ""
        lines.append(f'{pad}StructField("{f.name}", {type_src}, True{meta_arg}),')
    lines.append("])")
    return "\n".join(lines)


def dml_text_to_struct_source(text: str, *, indent: int = 4) -> str:
    return schema_to_struct_source(parse_dml(text), indent=indent)
