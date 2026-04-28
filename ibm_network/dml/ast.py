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
class DmlString:
    length: int | None = None
    delimiter: str | None = None


@dataclass(frozen=True)
class DmlDate:
    format: str = "yyyy-MM-dd"


@dataclass(frozen=True)
class DmlDatetime:
    format: str = "yyyy-MM-dd HH:mm:ss"


DmlScalar = DmlDecimal | DmlInteger | DmlString | DmlDate | DmlDatetime


@dataclass(frozen=True)
class DmlField:
    name: str
    type: DmlScalar


@dataclass(frozen=True)
class DmlRecord:
    fields: tuple[DmlField, ...]
