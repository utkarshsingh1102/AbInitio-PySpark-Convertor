"""How a source component's data file should be read into a Spark DataFrame.

The strategy is decided by inspecting the parsed DML record. v1 only emits the
two CSV strategies; the other members are placeholders for Phase 3 (one TC each):

  CSV_MIXED_DELIM      tc_010   multiple per-field delimiters → text + split(regex)
  TEXT_SUBSTRING       tc_002   fixed-width records → spark.read.text + substring
  TEXT_SPLIT_REGEX     tc_015   variable-length vectors → text + split + slice
  BINARY_FILE_EBCDIC   tc_024   EBCDIC charset → spark.read.binaryFile + decode("Cp037")
  BINARY_FILE_PACKED   tc_020   packed_decimal → binaryFile + UDF unpack

The chooser is intentionally a pure function so it stays trivially testable.
"""

from __future__ import annotations

from enum import Enum

from ibm_network.dml.ast import DmlDecimal, DmlField, DmlInteger, DmlRecord, DmlString


class ReadStrategy(Enum):
    CSV_DELIMITED = "CSV_DELIMITED"
    CSV_INFER = "CSV_INFER"
    CSV_MIXED_DELIM = "CSV_MIXED_DELIM"
    TEXT_SUBSTRING = "TEXT_SUBSTRING"
    TEXT_SPLIT_REGEX = "TEXT_SPLIT_REGEX"
    BINARY_FILE_EBCDIC = "BINARY_FILE_EBCDIC"
    BINARY_FILE_PACKED = "BINARY_FILE_PACKED"


def choose(record: DmlRecord | None) -> ReadStrategy:
    """Pick a read strategy for the given parsed DML record.

    Decision tree:
      - no record               → CSV_INFER (Spark guesses types)
      - any field has delimiter → CSV_DELIMITED (TC-001 / TC-007 / etc.)
      - all fields fixed-width  → TEXT_SUBSTRING (TC-002)
      - otherwise               → CSV_DELIMITED (conservative default)

    Mixed delimiters, variable vectors, EBCDIC, packed decimals will be
    routed to their dedicated strategies in later TCs.
    """
    if record is None:
        return ReadStrategy.CSV_INFER
    if any(_has_delimiter(f) for f in record.fields):
        return ReadStrategy.CSV_DELIMITED
    if record.fields and all(_is_fixed_width(f) for f in record.fields):
        return ReadStrategy.TEXT_SUBSTRING
    return ReadStrategy.CSV_DELIMITED


def _has_delimiter(field: DmlField) -> bool:
    t = field.type
    return isinstance(t, (DmlString, DmlDecimal)) and t.delimiter is not None


def _is_fixed_width(field: DmlField) -> bool:
    t = field.type
    if isinstance(t, DmlString):
        return t.length is not None
    if isinstance(t, DmlDecimal):
        return t.precision is not None and t.delimiter is None
    if isinstance(t, DmlInteger):
        return True
    return False
