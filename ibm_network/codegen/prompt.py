"""Build the tech-spec prompt for the optional LLM polish pass.

The polish pass takes the deterministically-synthesized PySpark file and asks the
local LLM to clean it up: collapse trivial chains, add docstrings, name intermediate
variables more meaningfully. It MUST NOT change semantics — the prompt is explicit.
"""

from __future__ import annotations

import textwrap

from ibm_network.ir.models import Graph

POLISH_SYSTEM = (
    "You are a senior PySpark engineer. Improve readability of a generated PySpark "
    "script without changing its semantics. Output only the revised Python code, no "
    "markdown, no commentary."
)


def build_polish_prompt(graph: Graph, generated_code: str) -> str:
    component_summary = "\n".join(
        f"  - {c.id} ({c.ab_initio_type}): {c.name}" for c in graph.components
    )
    return textwrap.dedent(
        """\
        Source Ab Initio graph: {graph_name}
        Components:
        {summary}

        Improve the PySpark script below. Rules:
          1. Do NOT change semantics — same DataFrame transformations, same order.
          2. Do NOT add or remove imports.
          3. Keep `build_pipeline(spark)` and `main()` as the entry points.
          4. You may add docstrings, rename intermediate variables, and re-flow long expressions.

        --- BEGIN SCRIPT ---
        {code}
        --- END SCRIPT ---
        """
    ).format(graph_name=graph.name, summary=component_summary, code=generated_code)
