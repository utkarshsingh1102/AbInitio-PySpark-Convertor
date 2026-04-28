"""DML schema-conversion suite harness.

Three parametrized tests per fixture (tc_001..tc_025):

  (a) test_schema[tc_NNN]  — generated StructType matches expected_schema.json
  (b) test_read[tc_NNN]    — generated read code, executed on sample_data,
                              produces expected_output.json
  (c) test_warning[tc_NNN] — only for tc_019/020/024: a MANUAL_REVIEW marker
                              must be emitted (currently fails for all three —
                              we have no warnings facility yet)

Phase 1 expectation: many failures. That's the intended baseline.
"""

from __future__ import annotations

import datetime
import decimal
import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from ibm_network.codegen import synthesize
from ibm_network.dml.parser import parse_dml
from ibm_network.dml.type_mapper import to_struct
from ibm_network.ir.models import Component, DMLRef, Edge, Graph, Port

FIX = Path(__file__).parent / "fixtures" / "dml_suite"
TC_IDS = sorted(d.name for d in FIX.iterdir() if d.is_dir() and d.name.startswith("tc_"))
WARNING_TCS = ["tc_019", "tc_020", "tc_024"]


# ---- shared helpers ---------------------------------------------------------

def _read_dml(tc_id: str) -> str:
    return (FIX / tc_id / "input.dml").read_text()


def _expected_schema(tc_id: str) -> dict:
    return json.loads((FIX / tc_id / "expected_schema.json").read_text())


def _expected_output(tc_id: str) -> list[dict[str, Any]]:
    return json.loads((FIX / tc_id / "expected_output.json").read_text())


def _sample_data_path(tc_id: str) -> Path | None:
    for ext in (".csv", ".dat"):
        p = FIX / tc_id / f"sample_data{ext}"
        if p.exists():
            return p
    return None


def _normalize(value: Any) -> Any:
    """Convert Spark scalars to JSON-comparable forms (matching expected_output.json).

    FloatType columns store values in 32-bit precision so a Python literal like
    ``98.6`` round-trips as ``98.5999984741...``. We round to 4 decimal places to
    keep TC-004's read-side comparison stable while still catching real bugs.
    """
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float):
        return round(value, 4)
    return value


def _build_single_source_graph(tc_id: str, sample_path: Path) -> Graph:
    return Graph(
        name=tc_id,
        components=[
            Component(
                id="src",
                ab_initio_type="INPUT_FILE",
                name="src",
                params={"input_path": str(sample_path)},
                out_ports=[Port(name="out", dml_ref="rec")],
                dml_refs=[DMLRef(name="rec", raw_text=_read_dml(tc_id))],
            ),
        ],
        edges=[],
    )


# ---- session-scoped Spark ---------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    spark = (
        pyspark_sql.SparkSession.builder
        .appName("ibm-net-dml-suite")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ---- (a) schema -------------------------------------------------------------

@pytest.mark.parametrize("tc_id", TC_IDS)
def test_schema(tc_id: str) -> None:
    record = parse_dml(_read_dml(tc_id))
    schema = to_struct(record)
    assert schema.jsonValue() == _expected_schema(tc_id)


# ---- (b) read ---------------------------------------------------------------

@pytest.mark.parametrize("tc_id", TC_IDS)
def test_read(tc_id: str, spark) -> None:
    sample = _sample_data_path(tc_id)
    if sample is None:
        pytest.skip(f"{tc_id}: no sample_data file")
    graph = _build_single_source_graph(tc_id, sample)
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)

    namespace: dict[str, Any] = {}
    exec(compile(result.code, f"<{tc_id}>", "exec"), namespace)
    df = namespace["build_pipeline"](spark)
    actual = [_normalize(row.asDict(recursive=True)) for row in df.collect()]
    assert actual == _expected_output(tc_id)


# ---- (c) warnings -----------------------------------------------------------

@pytest.mark.parametrize("tc_id", WARNING_TCS)
def test_warning(tc_id: str) -> None:
    sample = _sample_data_path(tc_id)
    graph = _build_single_source_graph(tc_id, sample) if sample else Graph(
        name=tc_id,
        components=[Component(
            id="src", ab_initio_type="INPUT_FILE", name="src",
            params={"input_path": "/tmp/dummy"},
            out_ports=[Port(name="out", dml_ref="rec")],
            dml_refs=[DMLRef(name="rec", raw_text=_read_dml(tc_id))],
        )],
        edges=[],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    notes_text = "\n".join(getattr(result, "notes", []) or [])
    warn_text = "\n".join(str(w.message) for w in caught)
    assert "MANUAL_REVIEW" in (notes_text + warn_text), (
        f"{tc_id}: expected MANUAL_REVIEW marker; "
        f"notes={result.notes!r} warnings={[str(w.message) for w in caught]!r}"
    )
