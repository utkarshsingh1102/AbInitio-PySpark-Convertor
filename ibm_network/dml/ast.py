from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DmlDecimal:
    precision: int | None = None
    scale: int = 0
    delimiter: str | None = None


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


@dataclass(frozen=True)
class DmlDatetime:
    format: str = "yyyy-MM-dd HH:mm:ss"


DmlScalar = DmlDecimal | DmlInteger | DmlReal | DmlString | DmlVoid | DmlDate | DmlDatetime


@dataclass(frozen=True)
class DmlField:
    name: str
    type: DmlScalar


@dataclass(frozen=True)
class DmlRecord:
    fields: tuple[DmlField, ...]
