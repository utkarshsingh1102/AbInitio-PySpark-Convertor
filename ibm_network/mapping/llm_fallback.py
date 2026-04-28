"""LLM fallback path — invoked when registry has no rule, or a transform expression
cannot be parsed deterministically.

The fallback is intentionally synchronous and *side-effect-free at import time*: it
constructs a prompt and asks the local LLM (Ollama / vLLM) to emit the PySpark snippet.
A `notes` entry is added to the resulting `Op` so downstream code can flag manual review.
"""

from __future__ import annotations

import textwrap

from ibm_network.codegen.llm_client import LLMClient, LLMUnavailableError
from ibm_network.ir.models import Component
from ibm_network.mapping.base import MappingError, Op, df_var

_FALLBACK_PROMPT = textwrap.dedent(
    """\
    You are translating an Ab Initio component into a single line of PySpark code.

    Component:
      type:   {ab_initio_type}
      id:     {component_id}
      name:   {component_name}
      params: {params}
      transform: {transform!r}

    Inputs (DataFrame variable names available in scope, in port order):
      {inputs}

    Emit ONLY the right-hand side of an assignment that produces a DataFrame.
    Use `F` as the alias for `pyspark.sql.functions`. Do not include imports,
    explanation, markdown, or trailing commentary. One line of Python only.
    """
)


def llm_map_component(
    comp: Component,
    inputs: list[str],
    *,
    client: LLMClient | None = None,
) -> Op:
    if client is None:
        client = LLMClient()
    prompt = _FALLBACK_PROMPT.format(
        ab_initio_type=comp.ab_initio_type,
        component_id=comp.id,
        component_name=comp.name,
        params=comp.params,
        transform=comp.transform,
        inputs=inputs,
    )
    try:
        body = client.generate(prompt).strip()
    except LLMUnavailableError as e:
        raise MappingError(f"LLM fallback unavailable for {comp.id!r}: {e}") from e

    code = _strip_code_fences(body)
    return Op(
        component_id=comp.id,
        var=df_var(comp.id),
        code=code,
        notes=[f"LLM fallback used for {comp.ab_initio_type} — manual review recommended"],
    )


def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        # remove leading fence (with optional language tag) and trailing fence
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text
