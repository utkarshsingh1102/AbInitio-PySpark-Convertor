from pathlib import Path

import pytest

from ibm_network.dml.ast import DmlDate, DmlDatetime, DmlDecimal, DmlInteger, DmlString
from ibm_network.dml.parser import parse_dml, parse_dml_file

FIX = Path(__file__).parent / "fixtures" / "dml"


def test_parses_customer_record() -> None:
    record = parse_dml_file(FIX / "customer.dml")
    assert [f.name for f in record.fields] == [
        "customer_id",
        "name",
        "signup_date",
        "last_login",
        "age",
        "balance",
    ]
    types = [f.type for f in record.fields]
    assert types[0] == DmlDecimal(precision=10, scale=0)
    assert types[1] == DmlString(length=20)
    assert types[2] == DmlDate(format="YYYY-MM-DD")
    assert types[3] == DmlDatetime(format="YYYYMMDDHHMMSS")
    assert types[4] == DmlInteger(size_bytes=4)
    assert types[5] == DmlDecimal(precision=12, scale=2)


def test_parses_delimited_record() -> None:
    record = parse_dml_file(FIX / "delimited.dml")
    types = [f.type for f in record.fields]
    assert types[0] == DmlDecimal(delimiter=",")
    assert types[1] == DmlString(delimiter="|")
    assert types[2] == DmlInteger(size_bytes=8)
    assert types[3] == DmlDecimal(delimiter=",")


def test_rejects_garbage() -> None:
    from lark.exceptions import LarkError
    with pytest.raises(LarkError):
        parse_dml("not a dml record")


def test_handles_inline_comments() -> None:
    text = """
    record
      // primary key
      decimal(10) id;
      /* the user's full name */
      string(50) name;
    end
    """
    record = parse_dml(text)
    assert [f.name for f in record.fields] == ["id", "name"]
