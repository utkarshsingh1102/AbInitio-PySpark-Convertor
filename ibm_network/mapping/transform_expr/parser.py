"""Lark-based parser for the DML transform sublanguage. Returns a generic `lark.Tree`,
which the codegen module walks directly — no separate AST layer (the tree is shallow)."""

from __future__ import annotations

from importlib.resources import files

from lark import Lark, Tree

_GRAMMAR_TEXT = (files("ibm_network.mapping.transform_expr") / "grammar.lark").read_text()

_BLOCK_PARSER = Lark(_GRAMMAR_TEXT, start="start_block", parser="earley")
_EXPR_PARSER = Lark(_GRAMMAR_TEXT, start="start_expr", parser="earley")


def parse_transform_block(text: str) -> Tree:
    """Parse a sequence of `out.field :: expr;` assignments."""
    return _BLOCK_PARSER.parse(text)


def parse_expr(text: str) -> Tree:
    """Parse a single DML expression (used for FILTER_BY_EXPRESSION's `select_expr`)."""
    return _EXPR_PARSER.parse(text)
