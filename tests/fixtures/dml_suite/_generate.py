"""Materialise the 25 DML test-case fixtures from DML_TEST_SUITE.md.

This module is the canonical specification of every TC's expected schema, expected
output rows, and sample data. Run it to (re-)generate the on-disk fixtures:

    python tests/fixtures/dml_suite/_generate.py

It writes, for each tc_001..tc_025:
  • input.dml             — DML schema verbatim from the suite
  • expected_schema.json  — pyspark StructType.jsonValue() output
  • sample_data.csv|.dat  — 3 sample records (CSV for delimited, .dat for fixed/binary)
  • expected_output.json  — DataFrame rows as a JSON list

The generator depends on pyspark (already installed in the test env) so that
expected_schema.json comes from the real `.jsonValue()` and is never hand-rolled.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ROOT = Path(__file__).parent

# WARNING_TCS — these are the suite's "degraded mapping" cases that must emit a
# MANUAL_REVIEW warning. Used by the harness assertion (c).
WARNING_TCS = {"tc_019", "tc_020", "tc_024"}


@dataclass
class Fixture:
    tc_id: str
    title: str
    dml: str
    schema: StructType
    sample_data: str = ""
    sample_data_ext: str = ".csv"
    expected_output: list[dict[str, Any]] = field(default_factory=list)
    extra_files: dict[str, str] = field(default_factory=dict)


FIXTURES: list[Fixture] = []


def add(**kw: Any) -> None:
    FIXTURES.append(Fixture(**kw))


# ───────────────────────── EASY ────────────────────────────────────────────

add(
    tc_id="tc_001",
    title="Basic delimited record",
    dml=(
        'record\n'
        '  decimal(",") customer_id;\n'
        '  string(",") first_name;\n'
        '  string(",") last_name;\n'
        '  string("\\n") email;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("customer_id", LongType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
    ]),
    sample_data="1,Alice,Smith,alice@example.com\n2,Bob,Jones,bob@x.com\n3,Carol,Lee,carol@y.com\n",
    expected_output=[
        {"customer_id": 1, "first_name": "Alice", "last_name": "Smith", "email": "alice@example.com"},
        {"customer_id": 2, "first_name": "Bob", "last_name": "Jones", "email": "bob@x.com"},
        {"customer_id": 3, "first_name": "Carol", "last_name": "Lee", "email": "carol@y.com"},
    ],
)

add(
    tc_id="tc_002",
    title="Fixed-length record",
    dml=(
        'record\n'
        '  string(10) emp_id;\n'
        '  string(30) emp_name;\n'
        '  decimal(5) age;\n'
        '  string(2) country_code;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("emp_id", StringType()),
        StructField("emp_name", StringType()),
        StructField("age", LongType()),
        StructField("country_code", StringType()),
    ]),
    sample_data_ext=".dat",
    sample_data=(
        # 10 + 30 + 5 + 2 = 47 cols per row
        "EMP0000001Alice                         00035US\n"
        "EMP0000002Bob                           00042CA\n"
        "EMP0000003Carol Smith                   00029GB\n"
    ),
    expected_output=[
        {"emp_id": "EMP0000001", "emp_name": "Alice                         ", "age": 35, "country_code": "US"},
        {"emp_id": "EMP0000002", "emp_name": "Bob                           ", "age": 42, "country_code": "CA"},
        {"emp_id": "EMP0000003", "emp_name": "Carol Smith                   ", "age": 29, "country_code": "GB"},
    ],
)

add(
    tc_id="tc_003",
    title="Decimal precision and scale",
    dml=(
        'record\n'
        '  decimal(",") order_id;\n'
        '  decimal("8.2", ",") unit_price;\n'
        '  decimal("12.4", ",") total_amount;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("order_id", LongType()),
        StructField("unit_price", DecimalType(8, 2)),
        StructField("total_amount", DecimalType(12, 4)),
    ]),
    sample_data="100,9.99,99.9000\n101,12.50,250.0000\n102,1.05,2.1000\n",
    expected_output=[
        {"order_id": 100, "unit_price": "9.99", "total_amount": "99.9000"},
        {"order_id": 101, "unit_price": "12.50", "total_amount": "250.0000"},
        {"order_id": 102, "unit_price": "1.05", "total_amount": "2.1000"},
    ],
)

add(
    tc_id="tc_004",
    title="Real / floating point",
    dml=(
        'record\n'
        '  decimal(",") sensor_id;\n'
        '  real("4", ",") temperature;\n'
        '  real("8", ",") pressure;\n'
        '  string("\\n") unit;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("sensor_id", LongType()),
        StructField("temperature", FloatType()),
        StructField("pressure", DoubleType()),
        StructField("unit", StringType()),
    ]),
    sample_data="1,98.6,1013.25,F\n2,21.4,1013.25,C\n3,5.0,995.0,C\n",
    expected_output=[
        {"sensor_id": 1, "temperature": 98.6, "pressure": 1013.25, "unit": "F"},
        {"sensor_id": 2, "temperature": 21.4, "pressure": 1013.25, "unit": "C"},
        {"sensor_id": 3, "temperature": 5.0, "pressure": 995.0, "unit": "C"},
    ],
)

add(
    tc_id="tc_005",
    title="Void / skip fields",
    dml=(
        'record\n'
        '  decimal(",") id;\n'
        '  void(",") padding1;\n'
        '  string(",") name;\n'
        '  void(",") padding2;\n'
        '  string("\\n") status;\n'
        'end;\n'
    ),
    # void fields are dropped from the output schema
    schema=StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("status", StringType()),
    ]),
    sample_data="1,xx,Alice,yy,active\n2,xx,Bob,yy,inactive\n3,xx,Carol,yy,active\n",
    expected_output=[
        {"id": 1, "name": "Alice", "status": "active"},
        {"id": 2, "name": "Bob", "status": "inactive"},
        {"id": 3, "name": "Carol", "status": "active"},
    ],
)

# ───────────────────────── MEDIUM ──────────────────────────────────────────

add(
    tc_id="tc_006",
    title="Nullable fields with null indicators",
    dml=(
        'record\n'
        '  decimal(",", null("")) customer_id;\n'
        '  string(",", null("NULL")) middle_name;\n'
        '  decimal("10.2", ",", null("-1")) discount;\n'
        '  string("\\n") status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("customer_id", LongType()),
        StructField("middle_name", StringType()),
        StructField("discount", DecimalType(10, 2)),
        StructField("status", StringType()),
    ]),
    sample_data="1,A,5.00,active\n,NULL,-1,inactive\n3,B,2.50,active\n",
    expected_output=[
        {"customer_id": 1, "middle_name": "A", "discount": "5.00", "status": "active"},
        {"customer_id": None, "middle_name": None, "discount": None, "status": "inactive"},
        {"customer_id": 3, "middle_name": "B", "discount": "2.50", "status": "active"},
    ],
)

add(
    tc_id="tc_007",
    title="Date and datetime types",
    dml=(
        'record\n'
        '  decimal(",") txn_id;\n'
        '  date("YYYY-MM-DD")(",") txn_date;\n'
        '  datetime("YYYYMMDDHH24MISS")(",") created_ts;\n'
        '  datetime("YYYY-MM-DD HH24:MI:SS")("\\n") updated_ts;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("txn_id", LongType()),
        StructField("txn_date", DateType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]),
    sample_data=(
        "1,2024-01-15,20240115093000,2024-01-15 10:30:00\n"
        "2,2024-02-29,20240229120015,2024-03-01 00:00:00\n"
        "3,2024-12-31,20241231235959,2024-12-31 23:59:59\n"
    ),
    expected_output=[
        {"txn_id": 1, "txn_date": "2024-01-15", "created_ts": "2024-01-15 09:30:00",
         "updated_ts": "2024-01-15 10:30:00"},
        {"txn_id": 2, "txn_date": "2024-02-29", "created_ts": "2024-02-29 12:00:15",
         "updated_ts": "2024-03-01 00:00:00"},
        {"txn_id": 3, "txn_date": "2024-12-31", "created_ts": "2024-12-31 23:59:59",
         "updated_ts": "2024-12-31 23:59:59"},
    ],
)

add(
    tc_id="tc_008",
    title="Default values",
    dml=(
        'record\n'
        '  decimal(",") id;\n'
        '  string(",") name = "UNKNOWN";\n'
        '  decimal(",") qty = 0;\n'
        '  string("\\n") region = "GLOBAL";\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("qty", LongType()),
        StructField("region", StringType()),
    ]),
    sample_data="1,Alice,5,US\n2,,,EU\n3,,,\n",
    expected_output=[
        {"id": 1, "name": "Alice", "qty": 5, "region": "US"},
        {"id": 2, "name": "UNKNOWN", "qty": 0, "region": "EU"},
        {"id": 3, "name": "UNKNOWN", "qty": 0, "region": "GLOBAL"},
    ],
)

add(
    tc_id="tc_009",
    title="Fixed-length vectors of primitives",
    dml=(
        'record\n'
        '  decimal(",") student_id;\n'
        '  decimal(",")[5] subject_scores;\n'
        '  string(",")[3] favorite_subjects;\n'
        '  string("\\n") name;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("student_id", LongType()),
        StructField("subject_scores", ArrayType(LongType())),
        StructField("favorite_subjects", ArrayType(StringType())),
        StructField("name", StringType()),
    ]),
    sample_data=(
        "1,80,90,75,88,92,Math,Sci,Art,Alice\n"
        "2,70,65,72,80,78,Eng,Hist,Geo,Bob\n"
    ),
    expected_output=[
        {"student_id": 1, "subject_scores": [80, 90, 75, 88, 92],
         "favorite_subjects": ["Math", "Sci", "Art"], "name": "Alice"},
        {"student_id": 2, "subject_scores": [70, 65, 72, 80, 78],
         "favorite_subjects": ["Eng", "Hist", "Geo"], "name": "Bob"},
    ],
)

add(
    tc_id="tc_010",
    title="Mixed delimiters",
    dml=(
        'record\n'
        '  decimal("|") account_id;\n'
        '  string("|") account_holder;\n'
        '  decimal("8.2", "|") balance;\n'
        '  date("YYYY-MM-DD")(";") opened_date;\n'
        '  string("\\n") branch;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("account_id", LongType()),
        StructField("account_holder", StringType()),
        StructField("balance", DecimalType(8, 2)),
        StructField("opened_date", DateType()),
        StructField("branch", StringType()),
    ]),
    sample_data=(
        "100|Alice|1500.50|2020-01-15;NYC\n"
        "101|Bob|750.25|2021-06-30;SFO\n"
    ),
    expected_output=[
        {"account_id": 100, "account_holder": "Alice", "balance": "1500.50",
         "opened_date": "2020-01-15", "branch": "NYC"},
        {"account_id": 101, "account_holder": "Bob", "balance": "750.25",
         "opened_date": "2021-06-30", "branch": "SFO"},
    ],
)

add(
    tc_id="tc_011",
    title="String with explicit length AND delimiter",
    dml=(
        'record\n'
        '  string(5, ",") product_code;\n'
        '  string(50, ",") product_name;\n'
        '  decimal("10.2", "\\n") price;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("product_code", StringType()),
        StructField("product_name", StringType()),
        StructField("price", DecimalType(10, 2)),
    ]),
    sample_data="ABC01,Widget,9.99\nABC02,Gizmo,19.95\nXYZ99,Sprocket,3.50\n",
    expected_output=[
        {"product_code": "ABC01", "product_name": "Widget", "price": "9.99"},
        {"product_code": "ABC02", "product_name": "Gizmo", "price": "19.95"},
        {"product_code": "XYZ99", "product_name": "Sprocket", "price": "3.50"},
    ],
)

add(
    tc_id="tc_012",
    title="Negative decimal indicators",
    dml=(
        'record\n'
        '  decimal(",") txn_id;\n'
        '  decimal("10.2", ",") signed_amount;\n'
        '  decimal("8", ",", null("0")) qty;\n'
        '  string("\\n") currency;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("txn_id", LongType()),
        StructField("signed_amount", DecimalType(10, 2)),
        StructField("qty", LongType()),
        StructField("currency", StringType()),
    ]),
    sample_data="1,100.50,5,USD\n2,-25.00,0,EUR\n3,-1000.00,3,GBP\n",
    expected_output=[
        {"txn_id": 1, "signed_amount": "100.50", "qty": 5, "currency": "USD"},
        {"txn_id": 2, "signed_amount": "-25.00", "qty": None, "currency": "EUR"},
        {"txn_id": 3, "signed_amount": "-1000.00", "qty": 3, "currency": "GBP"},
    ],
)

# ───────────────────────── HARD ────────────────────────────────────────────

add(
    tc_id="tc_013",
    title="Nested records",
    dml=(
        'record\n'
        '  decimal(",") customer_id;\n'
        '  string(",") name;\n'
        '  record\n'
        '    string(",") street;\n'
        '    string(",") city;\n'
        '    string(",") state;\n'
        '    string(",") zip;\n'
        '  end address;\n'
        '  string("\\n") phone;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", StringType()),
        ])),
        StructField("phone", StringType()),
    ]),
    sample_data=(
        "1,Alice,123 Main,Springfield,IL,62704,555-0101\n"
        "2,Bob,456 Oak,Madison,WI,53703,555-0202\n"
    ),
    expected_output=[
        {"customer_id": 1, "name": "Alice",
         "address": {"street": "123 Main", "city": "Springfield", "state": "IL", "zip": "62704"},
         "phone": "555-0101"},
        {"customer_id": 2, "name": "Bob",
         "address": {"street": "456 Oak", "city": "Madison", "state": "WI", "zip": "53703"},
         "phone": "555-0202"},
    ],
)

add(
    tc_id="tc_014",
    title="Deeply nested records",
    dml=(
        'record\n'
        '  decimal(",") id;\n'
        '  record\n'
        '    string(",") name;\n'
        '    record\n'
        '      string(",") street;\n'
        '      record\n'
        '        string(",") city;\n'
        '        string(",") country;\n'
        '      end location;\n'
        '    end address;\n'
        '  end customer;\n'
        '  string("\\n") status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("id", LongType()),
        StructField("customer", StructType([
            StructField("name", StringType()),
            StructField("address", StructType([
                StructField("street", StringType()),
                StructField("location", StructType([
                    StructField("city", StringType()),
                    StructField("country", StringType()),
                ])),
            ])),
        ])),
        StructField("status", StringType()),
    ]),
    sample_data=(
        "1,Alice,123 Main,Springfield,US,active\n"
        "2,Bob,456 Oak,Berlin,DE,inactive\n"
    ),
    expected_output=[
        {"id": 1, "customer": {"name": "Alice", "address": {
            "street": "123 Main", "location": {"city": "Springfield", "country": "US"}}},
         "status": "active"},
        {"id": 2, "customer": {"name": "Bob", "address": {
            "street": "456 Oak", "location": {"city": "Berlin", "country": "DE"}}},
         "status": "inactive"},
    ],
)

add(
    tc_id="tc_015",
    title="Variable-length vectors",
    dml=(
        'record\n'
        '  decimal(",") order_id;\n'
        '  decimal(",") item_count;\n'
        '  string(",")[item_count] item_names;\n'
        '  decimal(",")[item_count] item_quantities;\n'
        '  string("\\n") order_status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("order_id", LongType()),
        StructField("item_count", LongType()),
        StructField("item_names", ArrayType(StringType())),
        StructField("item_quantities", ArrayType(LongType())),
        StructField("order_status", StringType()),
    ]),
    sample_data=(
        "100,3,apple,banana,cherry,5,2,3,shipped\n"
        "101,2,milk,bread,1,2,pending\n"
    ),
    expected_output=[
        {"order_id": 100, "item_count": 3, "item_names": ["apple", "banana", "cherry"],
         "item_quantities": [5, 2, 3], "order_status": "shipped"},
        {"order_id": 101, "item_count": 2, "item_names": ["milk", "bread"],
         "item_quantities": [1, 2], "order_status": "pending"},
    ],
)

add(
    tc_id="tc_016",
    title="Vector of records",
    dml=(
        'record\n'
        '  decimal(",") order_id;\n'
        '  decimal(",") line_count;\n'
        '  record[line_count]\n'
        '    string(",") sku;\n'
        '    decimal(",") qty;\n'
        '    decimal("8.2", ",") price;\n'
        '  end line_items;\n'
        '  string("\\n") status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("order_id", LongType()),
        StructField("line_count", LongType()),
        StructField("line_items", ArrayType(StructType([
            StructField("sku", StringType()),
            StructField("qty", LongType()),
            StructField("price", DecimalType(8, 2)),
        ]))),
        StructField("status", StringType()),
    ]),
    sample_data="100,2,A,3,9.99,B,1,4.50,paid\n",
    expected_output=[
        {"order_id": 100, "line_count": 2,
         "line_items": [
             {"sku": "A", "qty": 3, "price": "9.99"},
             {"sku": "B", "qty": 1, "price": "4.50"},
         ],
         "status": "paid"},
    ],
)

add(
    tc_id="tc_017",
    title="Conditional records (if-then)",
    dml=(
        'record\n'
        '  decimal(",") record_type;\n'
        '  if (record_type == 1)\n'
        '    record\n'
        '      string(",") customer_name;\n'
        '      decimal(",") customer_age;\n'
        '    end customer_info;\n'
        '  if (record_type == 2)\n'
        '    record\n'
        '      decimal(",") order_id;\n'
        '      decimal("10.2", ",") order_amount;\n'
        '    end order_info;\n'
        '  string("\\n") timestamp;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("record_type", LongType()),
        StructField("customer_info", StructType([
            StructField("customer_name", StringType()),
            StructField("customer_age", LongType()),
        ])),
        StructField("order_info", StructType([
            StructField("order_id", LongType()),
            StructField("order_amount", DecimalType(10, 2)),
        ])),
        StructField("timestamp", StringType()),
    ]),
    sample_data="1,Alice,30,2024-01-01\n2,500,99.99,2024-01-02\n",
    expected_output=[
        {"record_type": 1,
         "customer_info": {"customer_name": "Alice", "customer_age": 30},
         "order_info": None,
         "timestamp": "2024-01-01"},
        {"record_type": 2,
         "customer_info": None,
         "order_info": {"order_id": 500, "order_amount": "99.99"},
         "timestamp": "2024-01-02"},
    ],
)

add(
    tc_id="tc_018",
    title="If-else chains",
    dml=(
        'record\n'
        '  string(",") status_code;\n'
        '  if (status_code == "A")\n'
        '    string(",") active_reason;\n'
        '  else if (status_code == "I")\n'
        '    string(",") inactive_reason;\n'
        '  else\n'
        '    string(",") generic_reason;\n'
        '  string("\\n") updated_by;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("status_code", StringType()),
        StructField("active_reason", StringType()),
        StructField("inactive_reason", StringType()),
        StructField("generic_reason", StringType()),
        StructField("updated_by", StringType()),
    ]),
    sample_data="A,onboarded,admin\nI,closed,system\nP,pending,system\n",
    expected_output=[
        {"status_code": "A", "active_reason": "onboarded",
         "inactive_reason": None, "generic_reason": None, "updated_by": "admin"},
        {"status_code": "I", "active_reason": None,
         "inactive_reason": "closed", "generic_reason": None, "updated_by": "system"},
        {"status_code": "P", "active_reason": None,
         "inactive_reason": None, "generic_reason": "pending", "updated_by": "system"},
    ],
)

add(
    tc_id="tc_019",
    title="Unions (degraded mapping)",
    dml=(
        'record\n'
        '  decimal(",") entity_id;\n'
        '  decimal(",") entity_type;\n'
        '  union\n'
        '    decimal(",") numeric_value;\n'
        '    string(",") string_value;\n'
        '    date("YYYY-MM-DD")(",") date_value;\n'
        '  end value;\n'
        '  string("\\n") source_system;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("entity_id", LongType()),
        StructField("entity_type", LongType()),
        StructField("value", StructType([
            StructField("numeric_value", LongType()),
            StructField("string_value", StringType()),
            StructField("date_value", DateType()),
        ])),
        StructField("source_system", StringType()),
    ]),
    sample_data="1,1,42,,,SYS_A\n2,2,,hello,,SYS_B\n3,3,,,2024-06-01,SYS_C\n",
    expected_output=[
        {"entity_id": 1, "entity_type": 1,
         "value": {"numeric_value": 42, "string_value": None, "date_value": None},
         "source_system": "SYS_A"},
        {"entity_id": 2, "entity_type": 2,
         "value": {"numeric_value": None, "string_value": "hello", "date_value": None},
         "source_system": "SYS_B"},
        {"entity_id": 3, "entity_type": 3,
         "value": {"numeric_value": None, "string_value": None, "date_value": "2024-06-01"},
         "source_system": "SYS_C"},
    ],
)

# ───────────────────────── EXPERT ──────────────────────────────────────────

add(
    tc_id="tc_020",
    title="Packed decimals (COMP-3)",
    dml=(
        'record\n'
        '  packed_decimal(5) account_num;\n'
        '  packed_decimal("7.2") balance;\n'
        '  zoned_decimal(4) status_code;\n'
        '  string(20) account_name;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("account_num", LongType()),
        StructField("balance", DecimalType(7, 2)),
        StructField("status_code", LongType()),
        StructField("account_name", StringType()),
    ]),
    sample_data_ext=".dat",
    # Placeholder bytes — real packed-decimal decoding requires a UDF.
    sample_data="\x00\x00\x12\x34\x5c\x00\x00\x12\x50\x0c\xf0\xf0\xf1ALICE               \n",
    expected_output=[
        {"account_num": 12345, "balance": "1250.00", "status_code": 1,
         "account_name": "ALICE               "},
    ],
)

add(
    tc_id="tc_021",
    title="Include / external schema reference",
    dml=(
        'include "common_address.dml";\n'
        '\n'
        'record\n'
        '  decimal(",") customer_id;\n'
        '  string(",") name;\n'
        '  address_t address;\n'
        '  string("\\n") phone;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", StringType()),
        ])),
        StructField("phone", StringType()),
    ]),
    sample_data="1,Alice,123 Main,Springfield,IL,62704,555-0101\n",
    expected_output=[
        {"customer_id": 1, "name": "Alice",
         "address": {"street": "123 Main", "city": "Springfield", "state": "IL", "zip": "62704"},
         "phone": "555-0101"},
    ],
    extra_files={
        "common_address.dml": (
            'type address_t = record\n'
            '  string(",") street;\n'
            '  string(",") city;\n'
            '  string(",") state;\n'
            '  string(",") zip;\n'
            'end;\n'
        ),
    },
)

add(
    tc_id="tc_022",
    title="Type aliases",
    dml=(
        'type money_t = decimal("12.2", ",");\n'
        'type ts_t = datetime("YYYY-MM-DD HH24:MI:SS")(",");\n'
        '\n'
        'record\n'
        '  decimal(",") txn_id;\n'
        '  money_t amount;\n'
        '  money_t fee;\n'
        '  ts_t created_at;\n'
        '  string("\\n") status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("txn_id", LongType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("fee", DecimalType(12, 2)),
        StructField("created_at", TimestampType()),
        StructField("status", StringType()),
    ]),
    sample_data="1,100.00,1.50,2024-01-15 09:30:00,settled\n",
    expected_output=[
        {"txn_id": 1, "amount": "100.00", "fee": "1.50",
         "created_at": "2024-01-15 09:30:00", "status": "settled"},
    ],
)

add(
    tc_id="tc_023",
    title="Comments and whitespace robustness",
    dml=(
        '/* Customer master record\n'
        '   Last updated: 2024-Q3 */\n'
        'record\n'
        '    decimal(",") customer_id;     // primary key\n'
        '\n'
        '    string(",")  name;            /* full name */\n'
        '    // skip middle name for now\n'
        '    decimal("10.2", ",") balance;\n'
        '    string("\\n") status;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("balance", DecimalType(10, 2)),
        StructField("status", StringType()),
    ]),
    sample_data="1,Alice,100.00,active\n2,Bob,250.50,inactive\n",
    expected_output=[
        {"customer_id": 1, "name": "Alice", "balance": "100.00", "status": "active"},
        {"customer_id": 2, "name": "Bob", "balance": "250.50", "status": "inactive"},
    ],
)

add(
    tc_id="tc_024",
    title="EBCDIC encoding hint",
    dml=(
        'record\n'
        '  string(10, charset="EBCDIC") emp_id;\n'
        '  string(30, charset="EBCDIC") emp_name;\n'
        '  packed_decimal(5) age;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("emp_id", StringType()),
        StructField("emp_name", StringType()),
        StructField("age", LongType()),
    ]),
    sample_data_ext=".dat",
    # EBCDIC-encoded "EMP00001" + Alice... + packed nibbles for 35
    sample_data="\xc5\xd4\xd7\xf0\xf0\xf0\xf0\xf1\xc1\xd3\xc9\xc3\xc5\x00\x00\x00\x35\x0c\n",
    expected_output=[
        {"emp_id": "EMP00001", "emp_name": "Alice", "age": 35},
    ],
)

add(
    tc_id="tc_025",
    title="Real-world combo (the boss fight)",
    dml=(
        'record\n'
        '  decimal(",") txn_id;\n'
        '  datetime("YYYY-MM-DD HH24:MI:SS")(",") txn_timestamp;\n'
        '  decimal(",") customer_id;\n'
        '  decimal(",") txn_type;\n'
        '\n'
        '  record\n'
        '    string(",", null("")) merchant_name;\n'
        '    string(",") merchant_category;\n'
        '    decimal("10.2", ",") amount;\n'
        '  end merchant_info;\n'
        '\n'
        '  decimal(",") item_count;\n'
        '  record[item_count]\n'
        '    string(",") sku;\n'
        '    decimal(",") quantity;\n'
        '    decimal("8.2", ",") line_total;\n'
        '  end line_items;\n'
        '\n'
        '  if (txn_type == 2)\n'
        '    record\n'
        '      decimal(",") original_txn_id;\n'
        '      string(",") refund_reason;\n'
        '    end refund_details;\n'
        '\n'
        '  string("\\n", null("UNKNOWN")) channel;\n'
        'end;\n'
    ),
    schema=StructType([
        StructField("txn_id", LongType()),
        StructField("txn_timestamp", TimestampType()),
        StructField("customer_id", LongType()),
        StructField("txn_type", LongType()),
        StructField("merchant_info", StructType([
            StructField("merchant_name", StringType()),
            StructField("merchant_category", StringType()),
            StructField("amount", DecimalType(10, 2)),
        ])),
        StructField("item_count", LongType()),
        StructField("line_items", ArrayType(StructType([
            StructField("sku", StringType()),
            StructField("quantity", LongType()),
            StructField("line_total", DecimalType(8, 2)),
        ]))),
        StructField("refund_details", StructType([
            StructField("original_txn_id", LongType()),
            StructField("refund_reason", StringType()),
        ])),
        StructField("channel", StringType()),
    ]),
    sample_data="1,2024-01-15 10:00:00,42,1,Acme,GROCERY,99.99,2,A,3,9.99,B,1,4.50,WEB\n",
    expected_output=[
        {"txn_id": 1, "txn_timestamp": "2024-01-15 10:00:00", "customer_id": 42, "txn_type": 1,
         "merchant_info": {"merchant_name": "Acme", "merchant_category": "GROCERY", "amount": "99.99"},
         "item_count": 2,
         "line_items": [
             {"sku": "A", "quantity": 3, "line_total": "9.99"},
             {"sku": "B", "quantity": 1, "line_total": "4.50"},
         ],
         "refund_details": None,
         "channel": "WEB"},
    ],
)


def main() -> None:
    assert len(FIXTURES) == 25, f"expected 25 fixtures, got {len(FIXTURES)}"
    for f in FIXTURES:
        d = ROOT / f.tc_id
        d.mkdir(parents=True, exist_ok=True)
        (d / "input.dml").write_text(f.dml)
        (d / "expected_schema.json").write_text(
            json.dumps(f.schema.jsonValue(), indent=2) + "\n"
        )
        if f.sample_data:
            (d / f"sample_data{f.sample_data_ext}").write_text(f.sample_data)
        (d / "expected_output.json").write_text(
            json.dumps(f.expected_output, indent=2, ensure_ascii=False) + "\n"
        )
        for name, content in f.extra_files.items():
            (d / name).write_text(content)
    print(f"generated {len(FIXTURES)} fixtures under {ROOT}")


if __name__ == "__main__":
    main()
