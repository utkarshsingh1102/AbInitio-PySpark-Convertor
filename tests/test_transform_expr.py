from ibm_network.mapping.transform_expr import expr_to_pyspark, transform_block_to_select_args


def test_simple_field_ref() -> None:
    assert expr_to_pyspark("in.amount") == 'F.col("amount")'


def test_arithmetic() -> None:
    src = expr_to_pyspark("in.amount + 1")
    assert "F.col(\"amount\")" in src
    assert "F.lit(1)" in src
    assert "+" in src


def test_comparison_uses_double_equals() -> None:
    src = expr_to_pyspark('in.status = "active"')
    assert "==" in src
    assert "F.lit(\"active\")" in src


def test_logical_and_uses_bitwise_and() -> None:
    src = expr_to_pyspark('in.amount > 100 and in.status = "active"')
    assert " & " in src


def test_string_concat_pipe_pipe() -> None:
    src = expr_to_pyspark("in.first || in.last")
    assert "F.concat(" in src


def test_if_then_else() -> None:
    src = expr_to_pyspark('if (in.score > 50) "high" else "low"')
    assert "F.when(" in src and ".otherwise(" in src


def test_builtin_substring() -> None:
    src = expr_to_pyspark("string_substring(in.name, 1, 5)")
    assert "F.substring(" in src and "1" in src and "5" in src


def test_is_null() -> None:
    src = expr_to_pyspark("is_null(in.x)")
    assert ".isNull()" in src


def test_transform_block_emits_aliases() -> None:
    block = """
        out.id   :: in.id;
        out.name :: string_upcase(in.name);
        out.flag :: if (in.score > 50) "high" else "low";
    """
    args = transform_block_to_select_args(block)
    assert len(args) == 3
    assert args[0].endswith('.alias("id")')
    assert "F.upper(" in args[1] and args[1].endswith('.alias("name")')
    assert "F.when(" in args[2] and args[2].endswith('.alias("flag")')
