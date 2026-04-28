from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from lark import Lark, Token, Transformer, Tree

from ibm_network.dml.ast import (
    DmlDate,
    DmlDatetime,
    DmlDecimal,
    DmlField,
    DmlInteger,
    DmlReal,
    DmlRecord,
    DmlScalar,
    DmlString,
    DmlVoid,
)

_GRAMMAR_TEXT = (files("ibm_network.dml") / "grammar.lark").read_text()
# LALR is faster than Earley and the v1 grammar is unambiguous; switching here so
# that the fixture suite (re-parses 25 inputs per run) doesn't pay Earley overhead.
_PARSER = Lark(_GRAMMAR_TEXT, start="start", parser="lalr")


@dataclass(frozen=True)
class _VectorMark:
    """Internal marker the field transformer uses to distinguish a parsed
    vector_suffix from a default literal (both can be ints)."""

    length: int | str


def _unquote(s: str) -> str:
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


def _partition_args(items: list[object]) -> tuple[list[str], str | None]:
    """Split a transformer's items list into positional ESCAPED_STRINGs (unquoted)
    and an optional ``null_indicator`` value. Tokens are unquoted; null_indicator
    Trees contribute their first child's unquoted string.
    """
    strings: list[str] = []
    null_value: str | None = None
    for it in items:
        if isinstance(it, Tree) and it.data == "null_indicator":
            null_value = _unquote(str(it.children[0]))
        else:
            strings.append(_unquote(str(it)))
    return strings, null_value


class _DMLTransformer(Transformer):
    def decimal_fixed(self, items: list[Token]) -> DmlDecimal:
        precision = int(items[0])
        scale = int(items[1]) if len(items) > 1 else 0
        return DmlDecimal(precision=precision, scale=scale)

    def decimal_str_form(self, items: list[object]) -> DmlDecimal:
        """Handle every string-form variant of decimal:

            decimal(delim)
            decimal(delim, null("..."))
            decimal("P.S", delim)
            decimal("P.S", delim, null("..."))
            decimal("P", delim)
        """
        strings, null_value = _partition_args(items)
        if len(strings) == 1:
            return DmlDecimal(delimiter=strings[0], null_value=null_value)
        precision_str = strings[0]
        delim = strings[1]
        if "." in precision_str:
            p_part, s_part = precision_str.split(".", 1)
            return DmlDecimal(
                precision=int(p_part),
                scale=int(s_part),
                delimiter=delim,
                null_value=null_value,
            )
        return DmlDecimal(precision=int(precision_str), delimiter=delim, null_value=null_value)

    def decimal_t(self, items: list[DmlDecimal]) -> DmlDecimal:
        return items[0]

    def integer_t(self, items: list[Token]) -> DmlInteger:
        return DmlInteger(size_bytes=int(items[0]))

    def real_t(self, items: list[Token]) -> DmlReal:
        size = int(_unquote(str(items[0])))
        delim = _unquote(str(items[1])) if len(items) > 1 else None
        return DmlReal(size_bytes=size, delimiter=delim)

    def string_fixed(self, items: list[Token]) -> DmlString:
        return DmlString(length=int(items[0]))

    def string_delim(self, items: list[object]) -> DmlString:
        strings, null_value = _partition_args(items)
        return DmlString(delimiter=strings[0], null_value=null_value)

    def string_t(self, items: list[DmlString]) -> DmlString:
        return items[0]

    def void_fixed(self, items: list[Token]) -> DmlVoid:
        return DmlVoid(length=int(items[0]))

    def void_delim(self, items: list[Token]) -> DmlVoid:
        return DmlVoid(delimiter=_unquote(str(items[0])))

    def void_t(self, items: list[DmlVoid]) -> DmlVoid:
        return items[0]

    def date_t(self, items: list[Token]) -> DmlDate:
        fmt = _unquote(str(items[0]))
        delim = _unquote(str(items[1])) if len(items) > 1 else None
        return DmlDate(format=fmt, delimiter=delim)

    def datetime_t(self, items: list[Token]) -> DmlDatetime:
        fmt = _unquote(str(items[0]))
        delim = _unquote(str(items[1])) if len(items) > 1 else None
        return DmlDatetime(format=fmt, delimiter=delim)

    def dml_type(self, items: list[DmlScalar]) -> DmlScalar:
        return items[0]

    def field(self, items: list[object]) -> DmlField:
        # field rule: dml_type vector_suffix? CNAME field_default?
        # Order is fixed but optional pieces drop out, so identify each item by type.
        ty = items[0]
        vector_length: int | str | None = None
        default: str | int | float | None = None
        name: str | None = None
        for it in items[1:]:
            if isinstance(it, _VectorMark):
                vector_length = it.length
            elif isinstance(it, Token):
                name = str(it)
            else:
                default = it  # type: ignore[assignment]
        assert name is not None, "field rule produced no CNAME"
        assert isinstance(
            ty, DmlDecimal | DmlInteger | DmlReal | DmlString | DmlVoid | DmlDate | DmlDatetime
        )
        return DmlField(name=name, type=ty, default=default, vector_length=vector_length)

    def vector_fixed(self, items: list[Token]) -> "_VectorMark":
        return _VectorMark(length=int(items[0]))

    def field_default(self, items: list[object]) -> str | int | float:
        # The single child is the parsed default_literal (str | int | float).
        return items[0]  # type: ignore[return-value]

    def default_string(self, items: list[Token]) -> str:
        return _unquote(str(items[0]))

    def default_number(self, items: list[Token]) -> int | float:
        text = str(items[0])
        if "." in text or "e" in text.lower():
            return float(text)
        return int(text)

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
