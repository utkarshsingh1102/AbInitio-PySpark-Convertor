from ibm_network.codegen.read_strategy import ReadStrategy, choose
from ibm_network.dml.ast import DmlDecimal, DmlField, DmlRecord, DmlString


def test_no_record_falls_back_to_infer() -> None:
    assert choose(None) is ReadStrategy.CSV_INFER


def test_delimited_record_picks_csv_delimited() -> None:
    rec = DmlRecord(fields=(DmlField(name="id", type=DmlDecimal(delimiter=",")),))
    assert choose(rec) is ReadStrategy.CSV_DELIMITED


def test_fixed_width_record_picks_text_substring() -> None:
    rec = DmlRecord(fields=(
        DmlField(name="id", type=DmlString(length=10)),
        DmlField(name="age", type=DmlDecimal(precision=5)),
    ))
    assert choose(rec) is ReadStrategy.TEXT_SUBSTRING


def test_enum_covers_all_phase3_targets() -> None:
    expected = {
        "CSV_DELIMITED", "CSV_INFER", "CSV_MIXED_DELIM",
        "TEXT_SUBSTRING", "TEXT_SPLIT_REGEX",
        "BINARY_FILE_EBCDIC", "BINARY_FILE_PACKED",
    }
    assert {s.value for s in ReadStrategy} == expected
