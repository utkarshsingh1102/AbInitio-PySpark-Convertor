from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DmlDecimal:
    precision: int | None = None
    scale: int = 0
    delimiter: str | None = None
    null_value: str | None = None
    # "decimal" (default), "packed" (TC-020 packed_decimal), or "zoned"
    # (TC-020 zoned_decimal). The synthesizer flips MANUAL_REVIEW when it sees
    # a non-default kind; the schema emitter ignores it (mainframe variants
    # land on the same Spark type as the equivalent decimal).
    kind: str = "decimal"


@dataclass(frozen=True)
class DmlInteger:
    size_bytes: int = 4


@dataclass(frozen=True)
class DmlReal:
    """IEEE-754 floating point. 4-byte → FloatType, 8-byte → DoubleType."""

    size_bytes: int = 8
    delimiter: str | None = None


@dataclass(frozen=True)
class DmlString:
    length: int | None = None
    delimiter: str | None = None
    null_value: str | None = None


@dataclass(frozen=True)
class DmlVoid:
    """`void(...)` — present in the source data for positional alignment but
    dropped from the output schema. See DML_TEST_SUITE TC-005.
    """

    length: int | None = None
    delimiter: str | None = None


@dataclass(frozen=True)
class DmlDate:
    format: str = "yyyy-MM-dd"
    delimiter: str | None = None


@dataclass(frozen=True)
class DmlDatetime:
    format: str = "yyyy-MM-dd HH:mm:ss"
    delimiter: str | None = None


DmlScalar = DmlDecimal | DmlInteger | DmlReal | DmlString | DmlVoid | DmlDate | DmlDatetime


@dataclass(frozen=True)
class DmlNested:
    """Inline sub-record (TC-013 / TC-014). Stored as a tuple of fields so the
    type continues to behave like the other immutable type nodes; recursion is
    expressed by a DmlField whose `type` is another DmlNested.
    """

    fields: tuple = ()  # tuple[DmlField, ...]  — recursive forward ref
    # `union ... end name;` parses to the same shape as a nested record, but
    # downstream consumers (warnings, eventually population logic) need to
    # know it was a union to flag it as MANUAL_REVIEW.
    is_union: bool = False


@dataclass(frozen=True)
class DmlCondition:
    """A single `column == value` test from an `if (...)` head (TC-017 / TC-018).

    The convertor only supports the `==` form today; richer comparison ops can
    plug in here without touching downstream codegen.
    """

    column: str
    op: str  # "=="
    value: str | int | float


@dataclass(frozen=True)
class DmlField:
    name: str
    type: DmlScalar | DmlNested
    default: str | int | float | None = None
    # Non-None when the field is a vector. Today only fixed-length (int) is
    # supported; TC-015's runtime length-prefixed form will plug an `str` here
    # naming the discriminator field.
    vector_length: int | str | None = None
    # Conditional-population metadata (TC-017 / TC-018):
    #   condition  → branch is active when this DmlCondition holds
    #   excludes   → branch is active when none of these conditions hold (else)
    #   is_else    → marks the branch as the unconditional `else` tail
    condition: DmlCondition | None = None
    excludes: tuple[DmlCondition, ...] = ()
    is_else: bool = False


@dataclass(frozen=True)
class DmlRecord:
    fields: tuple[DmlField, ...]
