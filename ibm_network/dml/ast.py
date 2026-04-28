from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DmlDecimal:
    precision: int | None = None
    scale: int = 0
    delimiter: str | None = None
    null_value: str | None = None


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


@dataclass(frozen=True)
class DmlField:
    name: str
    type: "DmlScalar | DmlNested"
    default: str | int | float | None = None
    # Non-None when the field is a vector. Today only fixed-length (int) is
    # supported; TC-015's runtime length-prefixed form will plug an `str` here
    # naming the discriminator field.
    vector_length: int | str | None = None


@dataclass(frozen=True)
class DmlRecord:
    fields: tuple[DmlField, ...]
