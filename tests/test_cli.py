"""CLI smoke test. We monkey-patch the Neo4j loader so this runs without infra."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from ibm_network import cli as cli_module
from ibm_network.ir.models import Component, DMLRef, Edge, Graph, Port


class _FakeLoader:
    def __init__(self, *_args: object, **_kw: object) -> None:
        pass

    def __enter__(self) -> "_FakeLoader":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def load_graph(self, name: str) -> Graph:
        return Graph(
            name=name,
            components=[
                Component(
                    id="src",
                    ab_initio_type="INPUT_FILE",
                    name="src",
                    params={"input_path": "/d/in.csv"},
                    out_ports=[Port(name="out", dml_ref="r")],
                    dml_refs=[DMLRef(name="r", raw_text="record decimal(8) id; end")],
                ),
                Component(
                    id="srt",
                    ab_initio_type="SORT",
                    name="srt",
                    params={"key": "id"},
                ),
            ],
            edges=[
                Edge(from_component="src", from_port="out", to_component="srt", to_port="in")
            ],
        )


def test_convert_writes_python_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(cli_module, "Neo4jGraphLoader", _FakeLoader)
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["convert", "sample_01", "--out", str(tmp_path), "--no-llm-fallback", "--no-polish"],
    )
    assert result.exit_code == 0, result.output
    out_file = tmp_path / "sample_01.py"
    assert out_file.exists()
    code = out_file.read_text()
    assert "build_pipeline" in code
    assert 'spark.read.csv("/d/in.csv"' in code
    assert "df_srt" in code
