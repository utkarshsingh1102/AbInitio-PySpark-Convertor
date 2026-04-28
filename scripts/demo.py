"""One-shot demo: build an Ab Initio graph in memory, synthesize it to PySpark, and
optionally execute the result against a tiny CSV fixture if pyspark is installed.

    python3 scripts/demo.py             # generate out/demo.py and print it
    python3 scripts/demo.py --run       # also execute it via PySpark if available
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import subprocess
import sys
from pathlib import Path

from ibm_network.codegen import synthesize
from ibm_network.ir.models import Component, DMLRef, Edge, Graph, Port

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "out"
DATA_PATH = OUT_DIR / "customers.csv"


def build_graph() -> Graph:
    """source(INPUT_FILE) -> REFORMAT -> FILTER -> ROLLUP."""
    customer_dml = "record decimal(10) id; string(20) name; integer(4) score; end"
    return Graph(
        name="demo",
        components=[
            Component(
                id="src",
                ab_initio_type="INPUT_FILE",
                name="customers_in",
                params={"input_path": str(DATA_PATH)},
                out_ports=[Port(name="out", dml_ref="cust")],
                dml_refs=[DMLRef(name="cust", raw_text=customer_dml)],
            ),
            Component(
                id="rfm",
                ab_initio_type="REFORMAT",
                name="reshape",
                transform=(
                    "out.id    :: in.id;"
                    'out.name  :: string_upcase(in.name);'
                    "out.score :: in.score;"
                    'out.tier  :: if (in.score > 50) "gold" else "silver";'
                ),
            ),
            Component(
                id="flt",
                ab_initio_type="FILTER_BY_EXPRESSION",
                name="only_gold",
                params={"select_expr": 'in.tier = "gold"'},
            ),
            Component(
                id="agg",
                ab_initio_type="ROLLUP",
                name="by_tier",
                params={"key": "tier"},
                transform="out.cnt :: count_recs(); out.total_score :: sum(in.score);",
            ),
        ],
        edges=[
            Edge(from_component="src", from_port="out", to_component="rfm", to_port="in"),
            Edge(from_component="rfm", from_port="out", to_component="flt", to_port="in"),
            Edge(from_component="flt", from_port="out", to_component="agg", to_port="in"),
        ],
    )


def write_sample_csv() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = [
        ("1", "alice", "80"),
        ("2", "bob", "30"),
        ("3", "carol", "75"),
        ("4", "dan", "10"),
        ("5", "eve", "90"),
    ]
    with DATA_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "score"])
        w.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="execute the generated script via pyspark")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_sample_csv()

    graph = build_graph()
    result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)

    target = OUT_DIR / "demo.py"
    target.write_text(result.code)
    print(f"[ok] wrote {target} ({len(result.code)} bytes)")
    print(f"[ok] sample data: {DATA_PATH}")
    if result.notes:
        print("[notes]")
        for n in result.notes:
            print(f"  - {n}")

    print("\n--- generated PySpark ---")
    print(result.code)

    if args.run:
        if importlib.util.find_spec("pyspark") is None:
            print("[skip] pyspark not installed — run `pip install pyspark` to execute")
            return 0
        print("--- executing via python (PySpark in local mode) ---")
        return subprocess.call([sys.executable, str(target)])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
