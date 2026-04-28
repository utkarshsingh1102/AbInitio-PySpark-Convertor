from __future__ import annotations

from importlib.resources import files
from pathlib import Path

from lark import Lark, Token, Transformer

from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlField,
    DmlInteger,
    DmlRecord,
    DmlScalar,
    DmlString,
)

_GRAMMAR_TEXT = (files("ibm_network.dml") / "grammar.lark").read_text()
# LALR is faster than Earley and the v1 grammar is unambiguous; switching here so
# that the fixture suite (re-parses 25 inputs per run) doesn't pay Earley overhead.
_PARSER = Lark(_GRAMMAR_TEXT, start="start", parser="lalr")


def _unquote(s: str) -> str:
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


class _DMLTransformer(Transformer):
    def decimal_fixed(self, items: list[Token]) -> DmlDecimal:
        precision = int(items[0])
        scale = int(items[1]) if len(items) > 1 else 0
        return DmlDecimal(precision=precision, scale=scale)

    def decimal_delim(self, items: list[Token]) -> DmlDecimal:
        return DmlDecimal(delimiter=_unquote(str(items[0])))

    def decimal_t(self, items: list[DmlDecimal]) -> DmlDecimal:
        return items[0]

    def integer_t(self, items: list[Token]) -> DmlInteger:
        return DmlInteger(size_bytes=int(items[0]))

    def string_fixed(self, items: list[Token]) -> DmlString:
        return DmlString(length=int(items[0]))

    def string_delim(self, items: list[Token]) -> DmlString:
        return DmlString(delimiter=_unquote(str(items[0])))

    def string_t(self, items: list[DmlString]) -> DmlString:
        return items[0]

    def date_t(self, items: list[Token]) -> DmlDate:
        return DmlDate(format=_unquote(str(items[0])))

    def datetime_t(self, items: list[Token]) -> DmlDatetime:
        return DmlDatetime(format=_unquote(str(items[0])))

    def dml_type(self, items: list[DmlScalar]) -> DmlScalar:
        return items[0]

    def field(self, items: list[object]) -> DmlField:
        ty = items[0]
        name = str(items[1])
        assert isinstance(ty, DmlDecimal | DmlInteger | DmlString | DmlDate | DmlDatetime)
        return DmlField(name=name, type=ty)

    def start(self, items: list[DmlField]) -> DmlRecord:
        return DmlRecord(fields=tuple(items))


def parse_dml(text: str) -> DmlRecord:
    """Parse a DML record-format string into an AST."""
    tree = _PARSER.parse(text)
    out = _DMLTransformer().transform(tree)
    assert isinstance(out, DmlRecord)
    return out


def parse_dml_file(path: str | Path) -> DmlRecord:
    return parse_dml(Path(path).read_text())
