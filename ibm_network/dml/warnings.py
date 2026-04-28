"""Centralised warning emission for unsupported / degraded DML mappings.

The DML pipeline has three categories of caller-visible diagnostics:

  1. **fallback** — we couldn't parse or map something, so we emitted a
     placeholder. Caller-visible text should be informative but not alarming;
     the user's input may be malformed or use a feature we don't yet support.

  2. **manual review** — we generated PySpark for an Ab Initio construct that
     has *no* exact Spark equivalent, so the output is a known-degraded mapping
     (unions → struct of nullables, packed_decimal → LongType + UDF stub,
     EBCDIC → binaryFile + decode). The text MUST contain the literal token
     ``MANUAL_REVIEW`` so the test suite (and any reviewer running grep) can
     find them quickly.

This module does not actually emit warnings — it builds structured `Warning`
records that callers (currently `codegen.synthesizer`) can format and surface
through whichever channel they choose (notes list, log line, stderr, etc).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WarningKind(Enum):
    """Categories of diagnostics. Members in `_MANUAL_REVIEW_KINDS` cause the
    formatted text to be prefixed with the literal ``MANUAL_REVIEW`` token.
    """

    # Fallback / parse-time
    DML_PARSE_FALLBACK = "DML_PARSE_FALLBACK"
    NO_DML_AVAILABLE = "NO_DML_AVAILABLE"
    UNMAPPABLE_COMPONENT = "UNMAPPABLE_COMPONENT"
    LLM_FALLBACK_FAILED = "LLM_FALLBACK_FAILED"
    LLM_POLISH_SKIPPED = "LLM_POLISH_SKIPPED"

    # Phase 3 — degraded but-known-correct mappings; require human sign-off
    UNION_DEGRADED = "UNION_DEGRADED"
    PACKED_DECIMAL_MANUAL = "PACKED_DECIMAL_MANUAL"
    ZONED_DECIMAL_MANUAL = "ZONED_DECIMAL_MANUAL"
    EBCDIC_MANUAL = "EBCDIC_MANUAL"
    MIXED_DELIM_FALLBACK = "MIXED_DELIM_FALLBACK"
    VARIABLE_VECTOR_FALLBACK = "VARIABLE_VECTOR_FALLBACK"


_MANUAL_REVIEW_KINDS: frozenset[WarningKind] = frozenset({
    WarningKind.UNION_DEGRADED,
    WarningKind.PACKED_DECIMAL_MANUAL,
    WarningKind.ZONED_DECIMAL_MANUAL,
    WarningKind.EBCDIC_MANUAL,
    WarningKind.MIXED_DELIM_FALLBACK,
    WarningKind.VARIABLE_VECTOR_FALLBACK,
})


@dataclass(frozen=True)
class DmlWarning:
    """A single diagnostic.

    Attributes:
        kind:      categorical tag for grouping / filtering.
        source_id: where the issue was detected — typically a component id or
                   fixture id, free-form string used by humans.
        detail:    free-form message describing the issue.
    """

    kind: WarningKind
    source_id: str
    detail: str

    def is_manual_review(self) -> bool:
        return self.kind in _MANUAL_REVIEW_KINDS

    def format(self) -> str:
        prefix = "MANUAL_REVIEW " if self.is_manual_review() else ""
        return f"{prefix}[{self.kind.value}] {self.source_id}: {self.detail}"


# ---- helper constructors used by synthesizer (kept terse on purpose) -------

def dml_parse_fallback(source_id: str, error: str) -> DmlWarning:
    return DmlWarning(WarningKind.DML_PARSE_FALLBACK, source_id,
                   f"DML parse failed ({error}); falling back to inferSchema=True")


def no_dml_available(source_id: str) -> DmlWarning:
    return DmlWarning(WarningKind.NO_DML_AVAILABLE, source_id,
                   "no DML attached to source component; falling back to inferSchema=True")


def unmappable_component(source_id: str, component_type: str, reason: str) -> DmlWarning:
    return DmlWarning(WarningKind.UNMAPPABLE_COMPONENT, source_id,
                   f"rule miss for {component_type} → LLM fallback ({reason})")


def llm_fallback_failed(source_id: str, error: str) -> DmlWarning:
    return DmlWarning(WarningKind.LLM_FALLBACK_FAILED, source_id,
                   f"LLM fallback unavailable ({error}); placeholder emitted")


def llm_polish_skipped(reason: str) -> DmlWarning:
    return DmlWarning(WarningKind.LLM_POLISH_SKIPPED, "polish", reason)
