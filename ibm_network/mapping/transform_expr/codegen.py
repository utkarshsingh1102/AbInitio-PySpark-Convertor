"""Translate a parsed DML transform tree into PySpark Column-expression source code."""

from __future__ import annotations

from collections.abc import Callable

from lark import Token, Tree

from ibm_network.mapping.transform_expr.parser import parse_expr, parse_transform_block

# DML builtin → PySpark function-source. Each entry is (formatter).
# `formatter(args_src)` returns the Python source expression.
_BUILTINS: dict[str, Callable[[list[str]], str]] = {
    "is_null": lambda a: f"({a[0]}).isNull()",
    "is_blank": lambda a: f"(({a[0]}).isNull() | ({a[0]} == F.lit('')))",
    "is_defined": lambda a: f"({a[0]}).isNotNull()",
    "string_concat": lambda a: f"F.concat({', '.join(a)})",
    "string_substring": lambda a: f"F.substring({a[0]}, {a[1]}, {a[2]})",
    "string_length": lambda a: f"F.length({a[0]})",
    "string_upcase": lambda a: f"F.upper({a[0]})",
    "string_downcase": lambda a: f"F.lower({a[0]})",
    "string_trim": lambda a: f"F.trim({a[0]})",
    "string_lrtrim": lambda a: f"F.trim({a[0]})",
    "decimal_round": lambda a: f"F.round({a[0]}, {a[1]})",
    "math_abs": lambda a: f"F.abs({a[0]})",
    "now": lambda _: "F.current_timestamp()",
    "today": lambda _: "F.current_date()",
    "coalesce": lambda a: f"F.coalesce({', '.join(a)})",
    # Aggregates (used by ROLLUP)
    "sum": lambda a: f"F.sum({a[0]})",
    "avg": lambda a: f"F.avg({a[0]})",
    "min": lambda a: f"F.min({a[0]})",
    "max": lambda a: f"F.max({a[0]})",
    "count": lambda a: f"F.count({a[0]})" if a else "F.count('*')",
    "count_recs": lambda _: "F.count('*')",
}

_CMP_MAP = {
    "==": "==",
    "=": "==",
    "!=": "!=",
    "<>": "!=",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
}


class TransformExprError(Exception):
    pass


def expr_to_pyspark(text: str) -> str:
    """Compile a single DML expression to a PySpark Column expression source string."""
    tree = parse_expr(text)
    # start_expr → expr
    inner = tree.children[0] if isinstance(tree, Tree) else tree
    return _emit(inner)


def transform_block_to_select_args(text: str) -> list[str]:
    """Compile a `out.x :: expr;` block into a list of `expr.alias("x")` source strings,
    suitable for splatting into `df.select(...)`.
    """
    tree = parse_transform_block(text)
    out: list[str] = []
    for child in tree.children:
        assert isinstance(child, Tree) and child.data == "assignment"
        target = str(child.children[0])
        # children[1] is the ASSIGN token, children[2] is the expression
        expr_node = child.children[2]
        rhs = _emit(expr_node)
        out.append(f'{rhs}.alias("{target}")')
    return out


def _emit(node: Tree | Token) -> str:
    if isinstance(node, Token):
        # bare identifier reaching this point is unusual; treat as F.col
        return f'F.col("{node}")'

    rule = node.data

    if rule == "number":
        return f"F.lit({node.children[0]})"

    if rule == "string_lit":
        return f"F.lit({node.children[0]})"

    if rule == "null_lit":
        return "F.lit(None)"

    if rule == "ident":
        name = str(node.children[0])
        if name.lower() in {"true", "false"}:
            return f"F.lit({name.capitalize()})"
        if name.lower() in {"null", "none"}:
            return "F.lit(None)"
        return f'F.col("{name}")'

    if rule == "field_ref":
        # e.g., in.field, in0.field — drop the qualifier; for v1 we assume single source
        field = str(node.children[1])
        return f'F.col("{field}")'

    if rule == "neg":
        return f"(-{_emit(node.children[0])})"

    if rule == "not_op":
        return f"(~{_emit(node.children[0])})"

    if rule == "and_op":
        a, b = (_emit(c) for c in node.children)
        return f"({a} & {b})"

    if rule == "or_op":
        a, b = (_emit(c) for c in node.children)
        return f"({a} | {b})"

    if rule == "cmp":
        left, op_tok, right = node.children
        op = _CMP_MAP[str(op_tok)]
        return f"({_emit(left)} {op} {_emit(right)})"

    if rule == "binop":
        left, op_tok, right = node.children
        op = str(op_tok)
        l_src, r_src = _emit(left), _emit(right)
        if op == "||":
            return f"F.concat({l_src}, {r_src})"
        return f"({l_src} {op} {r_src})"

    if rule == "if_then_else":
        cond, then_, else_ = node.children
        return f"F.when({_emit(cond)}, {_emit(then_)}).otherwise({_emit(else_)})"

    if rule == "call":
        fn = str(node.children[0])
        args_node = node.children[1] if len(node.children) > 1 else None
        args_src: list[str] = []
        if args_node is not None and isinstance(args_node, Tree):
            args_src = [_emit(c) for c in args_node.children]
        formatter = _BUILTINS.get(fn.lower())
        if formatter is None:
            raise TransformExprError(f"unsupported DML function: {fn}")
        return formatter(args_src)

    raise TransformExprError(f"unhandled node: {rule}")
