"""End-to-end synthesizer tests that don't require Neo4j or Ollama. We construct an IR
graph by hand, run `synthesize`, and assert the output parses as valid Python and
contains the expected lines.
"""

from __future__ import annotations

import ast

from ibm_network.codegen import synthesize
from ibm_network.ir.models import Component, DMLRef, Edge, Graph, Port


def _build_sample_graph() -> Graph:
    """source → reformat → filter → sort (sink). One DML attached to the source."""
    # Delimited DML so the source routes through ReadStrategy.CSV_DELIMITED and
    # emits a schema_var the assertions below check for.
    customer_dml = """
    record
      decimal(",") id;
      string(",") name;
      decimal(",") score;
    end
    """
    src = Component(
        id="src",
        ab_initio_type="INPUT_FILE",  # not in the registry — handled as a source read
        name="customer_in",
        params={"input_path": "/data/customers.csv"},
        out_ports=[Port(name="out", dml_ref="customer")],
        dml_refs=[DMLRef(name="customer", raw_text=customer_dml)],
    )
    reformat = Component(
        id="rfm",
        ab_initio_type="REFORMAT",
        name="reshape",
        transform="""
            out.id   :: in.id;
            out.name :: string_upcase(in.name);
            out.tier :: if (in.score > 50) "gold" else "silver";
        """,
    )
    flt = Component(
        id="flt",
        ab_initio_type="FILTER_BY_EXPRESSION",
        name="active_only",
        params={"select_expr": 'in.tier = "gold"'},
    )
    srt = Component(
        id="srt",
        ab_initio_type="SORT",
        name="sort_by_id",
        params={"key": "id asc"},
    )
    return Graph(
        name="sample_pipeline",
        components=[src, reformat, flt, srt],
        edges=[
            Edge(from_component="src", from_port="out", to_component="rfm", to_port="in"),
            Edge(from_component="rfm", from_port="out", to_component="flt", to_port="in"),
            Edge(from_component="flt", from_port="out", to_component="srt", to_port="in"),
        ],
    )


def test_synthesize_produces_valid_python() -> None:
    graph = _build_sample_graph()
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    # The generated code must be a complete, parseable Python module.
    ast.parse(result.code)


def test_synthesize_contains_expected_pieces() -> None:
    graph = _build_sample_graph()
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    code = result.code
    assert '.csv("/data/customers.csv"' in code
    assert "schema=schema_src" in code
    assert "df_src.select(" in code
    assert "F.upper(" in code
    assert 'F.when(' in code and ".otherwise(" in code
    assert "df_rfm.filter(" in code
    assert 'df_flt.orderBy(F.col("id").asc())' in code
    assert "def build_pipeline(" in code
    assert "if __name__" in code
    assert result.final_var == "df_srt"


def test_synthesize_includes_struct_for_source() -> None:
    graph = _build_sample_graph()
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    assert "schema_src = StructType([" in result.code
    assert 'StructField("id", LongType()' in result.code


def test_synthesize_topological_join() -> None:
    """JOIN with two upstream sources gets the inputs in port-order."""
    left = Component(
        id="L",
        ab_initio_type="INPUT_FILE",
        name="left",
        params={"input_path": "/d/l.csv"},
    )
    right = Component(
        id="R",
        ab_initio_type="INPUT_FILE",
        name="right",
        params={"input_path": "/d/r.csv"},
    )
    j = Component(id="J", ab_initio_type="JOIN", name="j", params={"key": "id"})
    graph = Graph(
        name="join_test",
        components=[left, right, j],
        edges=[
            Edge(from_component="L", from_port="out", to_component="J", to_port="in0"),
            Edge(from_component="R", from_port="out", to_component="J", to_port="in1"),
        ],
    )
    code = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False).code
    ast.parse(code)
    assert 'df_L.join(df_R, on=["id"], how="inner")' in code
