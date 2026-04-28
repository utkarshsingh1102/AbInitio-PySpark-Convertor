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

from ibm_network.dml.ast import DmlRecord


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

    Returns `CSV_INFER` if there is no parsed record (DML missing or unparseable),
    otherwise `CSV_DELIMITED`. Phase 3 will route fixed-width / mixed-delim /
    binary records to the matching strategy.
    """
    if record is None:
        return ReadStrategy.CSV_INFER
    return ReadStrategy.CSV_DELIMITED
