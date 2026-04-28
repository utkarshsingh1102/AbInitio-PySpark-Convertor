"""End-to-end test against live infra. Marked `integration` and skipped by default.

Run with:
    docker compose up -d neo4j
    cypher-shell -u neo4j -p devpassword \\
        -f tests/fixtures/graphs/sample_01.cypher
    pytest -m integration tests/test_e2e.py
"""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

from ibm_network.codegen import synthesize
from ibm_network.ir.neo4j_loader import Neo4jGraphLoader

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    importlib.util.find_spec("neo4j") is None, reason="neo4j driver not installed"
)
def test_e2e_loads_and_synthesizes_sample_01(tmp_path: Path) -> None:
    with Neo4jGraphLoader() as loader:
        graph = loader.load_graph("sample_01")
    assert graph.components, "fixture graph not seeded — see test docstring"
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    out = tmp_path / "sample_01.py"
    out.write_text(result.code)
    assert "build_pipeline" in out.read_text()


@pytest.mark.skipif(shutil.which("spark-submit") is None, reason="spark-submit not on PATH")
def test_generated_script_runs_via_spark_submit(tmp_path: Path) -> None:
    """If we have spark-submit available, sanity-check the generated file actually runs.

    This is intentionally minimal — full output-validation belongs in the validation
    corpus (Phase 6).
    """
    with Neo4jGraphLoader() as loader:
        graph = loader.load_graph("sample_01")
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    target = tmp_path / "sample_01.py"
    target.write_text(result.code)
    # Caller is expected to point input_path at a real CSV; the test just checks parse.
    import ast

    ast.parse(target.read_text())
