"""Walk an IR Graph, dispatch each component to the mapping registry, and render
the result through the Jinja pipeline template.

The synthesizer is deterministic. The LLM is consulted only:
  1. As a *fallback* when a component has no rule (gated by `enable_llm_fallback`).
  2. As an *optional polish pass* on the final source (gated by `Settings.enable_llm_polish`).
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from typing import Iterable

from jinja2 import Environment

from ibm_network.codegen.llm_client import LLMClient, LLMUnavailableError
from ibm_network.codegen.prompt import POLISH_SYSTEM, build_polish_prompt
from ibm_network.codegen.read_strategy import ReadStrategy, choose as choose_read_strategy
from ibm_network.codegen.source_emitter import render_source_read
from ibm_network.dml.ast import DmlDecimal, DmlRecord, DmlString
from ibm_network.dml.emitter import render_schema
from ibm_network.dml.parser import parse_dml
from ibm_network.dml.warnings import (
    dml_parse_fallback,
    llm_fallback_failed,
    llm_polish_skipped,
    no_dml_available,
    unmappable_component,
)
from ibm_network.ir.models import Component, Graph, Port
from ibm_network.mapping import map_component
from ibm_network.mapping.base import MappingError, Op, df_var
from ibm_network.mapping.llm_fallback import llm_map_component

_TEMPLATE_TEXT = (
    files("ibm_network.codegen.templates") / "pipeline.py.j2"
).read_text()
_JINJA = Environment(
    trim_blocks=False,
    lstrip_blocks=False,
    keep_trailing_newline=True,
)


@dataclass
class GeneratedScript:
    code: str
    notes: list[str]
    final_var: str


def synthesize(
    graph: Graph,
    *,
    enable_llm_fallback: bool = True,
    enable_llm_polish: bool = False,
    llm_client: LLMClient | None = None,
) -> GeneratedScript:
    ordered = graph.topological_order()
    if not ordered:
        raise ValueError(f"graph {graph.name!r} has no components")

    schemas: list[tuple[str, str]] = []
    reads: list[str] = []
    steps: list[dict[str, object]] = []
    notes: list[str] = []

    sources = {c.id for c in ordered if not graph.upstream(c.id)}

    for comp in ordered:
        upstream_edges = graph.upstream(comp.id)
        if comp.id in sources:
            schema_var, read_line, src_notes = _emit_source(comp)
            if schema_var is not None:
                schemas.append(schema_var)
            reads.append(read_line)
            notes.extend(src_notes)
            continue

        # ordering each input by `to_port` keeps JOIN's left/right deterministic
        ordered_edges = sorted(upstream_edges, key=lambda e: e.to_port)
        inputs = [df_var(e.from_component) for e in ordered_edges]

        op, op_notes = _emit_component(
            comp, inputs, enable_llm_fallback=enable_llm_fallback, llm_client=llm_client
        )
        notes.extend(op_notes)
        steps.append(
            {
                "var": op.var,
                "code": op.code,
                "notes": op.notes,
            }
        )

    # The conventional "result" is the last component without downstream edges.
    leaves = [c.id for c in ordered if not graph.downstream(c.id)]
    final_id = leaves[-1] if leaves else ordered[-1].id
    final_var = df_var(final_id)

    rendered = _JINJA.from_string(_TEMPLATE_TEXT).render(
        graph_name=graph.name,
        component_count=len(ordered),
        schemas=schemas,
        reads=reads,
        steps=steps,
        final_var=final_var,
        writes=[],
    )
    code = _ensure_trailing_newline(rendered)

    if enable_llm_polish:
        code = _maybe_polish(graph, code, llm_client, notes)

    return GeneratedScript(code=code, notes=notes, final_var=final_var)


def _emit_source(comp: Component) -> tuple[tuple[str, str] | None, str, list[str]]:
    """Build the read line for a source component.

    Parses the component's output DML (if any), asks `read_strategy.choose` for
    a strategy, and routes through `source_emitter.render_source_read` for the
    actual code text. Schema generation and the read line are decoupled so
    Phase 3 can route fixed-width / binary records to non-CSV strategies.
    """
    var = df_var(comp.id)
    input_path = comp.params.get("url") or comp.params.get("input_path") or f"<TODO:{comp.id}>"
    notes: list[str] = []

    out_dml = _first_dml(comp.dml_refs, comp.out_ports) or _first_dml(comp.dml_refs, comp.in_ports)
    record = None
    if out_dml is not None:
        try:
            record = parse_dml(out_dml)
        except Exception as e:  # pragma: no cover - parser errors surface to caller
            notes.append(dml_parse_fallback(comp.id, str(e)).format())
    else:
        notes.append(no_dml_available(comp.id).format())

    strategy = choose_read_strategy(record)
    schema_entry: tuple[str, str] | None = None
    schema_var: str | None = None
    if record is not None and strategy is ReadStrategy.CSV_DELIMITED:
        schema_var = f"schema_{_safe_id(comp.id)}"
        schema_entry = (schema_var, render_schema(record))

    delimiter = _record_delimiter(record) if record is not None else None
    read_body = render_source_read(
        strategy,
        schema_var=schema_var,
        input_path=input_path,
        delimiter=delimiter,
        record=record,
    )
    return schema_entry, f"{var} = {read_body}", notes


def _record_delimiter(record: DmlRecord) -> str | None:
    """Pick the field delimiter to use for CSV reads.

    Uses the first non-newline delimiter found among the record's fields. Mixed
    delimiters (TC-010) need a different strategy and are out of scope here.
    """
    for f in record.fields:
        t = f.type
        delim = t.delimiter if isinstance(t, (DmlString, DmlDecimal)) else None
        if delim and delim != "\n":
            return delim
    return None


def _emit_component(
    comp: Component,
    inputs: list[str],
    *,
    enable_llm_fallback: bool,
    llm_client: LLMClient | None,
) -> tuple[Op, list[str]]:
    notes: list[str] = []
    try:
        return map_component(comp, inputs), notes
    except MappingError as rule_err:
        if not enable_llm_fallback:
            raise
        notes.append(unmappable_component(comp.id, comp.ab_initio_type, str(rule_err)).format())
        try:
            return llm_map_component(comp, inputs, client=llm_client), notes
        except (MappingError, LLMUnavailableError) as e:
            placeholder = Op(
                component_id=comp.id,
                var=df_var(comp.id),
                code=f"{inputs[0] if inputs else 'spark.emptyDataFrame'}  # TODO: unmappable {comp.ab_initio_type}",
                notes=[f"unmappable: {rule_err}; fallback failed: {e}"],
            )
            notes.append(llm_fallback_failed(comp.id, str(e)).format())
            return placeholder, notes


def _maybe_polish(
    graph: Graph, code: str, llm_client: LLMClient | None, notes: list[str]
) -> str:
    client = llm_client or LLMClient()
    try:
        polished = client.generate(build_polish_prompt(graph, code), system=POLISH_SYSTEM)
    except LLMUnavailableError as e:
        notes.append(llm_polish_skipped(str(e)).format())
        return code
    polished = polished.strip()
    # If the model wrapped output in a markdown fence, strip it.
    if polished.startswith("```"):
        polished = "\n".join(
            line for line in polished.splitlines() if not line.strip().startswith("```")
        ).strip()
    if not polished or "build_pipeline" not in polished:
        notes.append(llm_polish_skipped("model output did not preserve entry points").format())
        return code
    return _ensure_trailing_newline(polished)


def _first_dml(dml_refs: Iterable, ports: Iterable[Port]) -> str | None:
    by_name = {d.name: d.raw_text for d in dml_refs}
    for p in ports:
        if p.dml_ref and p.dml_ref in by_name:
            return by_name[p.dml_ref]
    # fall back to first available DML
    for d in dml_refs:
        if d.raw_text:
            return d.raw_text
    return None


def _safe_id(component_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in component_id)


def _ensure_trailing_newline(s: str) -> str:
    return s if s.endswith("\n") else s + "\n"
