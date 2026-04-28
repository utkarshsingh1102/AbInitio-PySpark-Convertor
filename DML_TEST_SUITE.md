# DML Schema → PySpark Converter: Test Suite

A comprehensive test suite for validating an Ab Initio DML → PySpark schema converter.
Test cases are ordered from easy → hard and grouped by feature area.

---

## How to use this file

Each test case has:
- **ID** — stable identifier (use as test name)
- **Category** — feature area being tested
- **Difficulty** — 🟢 Easy / 🟡 Medium / 🔴 Hard / ⚫ Expert
- **DML Input** — the source DML schema
- **Expected PySpark Output** — the target `StructType` and any read logic
- **Notes** — edge cases, gotchas, what the test is really checking

---

## 🟢 EASY — Foundational Type Mapping

### TC-001: Basic delimited record
**Category:** Delimited parsing, primitive types
**DML:**
```
record
  decimal(",") customer_id;
  string(",") first_name;
  string(",") last_name;
  string("\n") email;
end;
```
**Expected:**
```python
StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
])
# Read: spark.read.option("delimiter", ",").csv(path, schema=schema)
```
**Notes:** Sanity check. Last field uses `\n` to mark end-of-record.

---

### TC-002: Fixed-length record
**Category:** Fixed-width parsing
**DML:**
```
record
  string(10) emp_id;
  string(30) emp_name;
  decimal(5) age;
  string(2) country_code;
end;
```
**Expected:**
```python
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("emp_name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("country_code", StringType(), True),
])
# Read raw lines, then:
df = (spark.read.text(path)
    .select(
        substring("value", 1, 10).alias("emp_id"),
        substring("value", 11, 30).alias("emp_name"),
        substring("value", 41, 5).cast("long").alias("age"),
        substring("value", 46, 2).alias("country_code"),
    ))
```
**Notes:** Converter must track running offset (1, 11, 41, 46…). PySpark `substring` is 1-indexed.

---

### TC-003: Decimal precision and scale
**Category:** Numeric types
**DML:**
```
record
  decimal(",") order_id;
  decimal("8.2", ",") unit_price;
  decimal("12.4", ",") total_amount;
end;
```
**Expected:**
```python
StructType([
    StructField("order_id", LongType(), True),
    StructField("unit_price", DecimalType(8, 2), True),
    StructField("total_amount", DecimalType(12, 4), True),
])
```
**Notes:** Plain `decimal(",")` with no precision → `LongType` (integer). With `"P.S"` → `DecimalType(P, S)`.

---

### TC-004: Real / floating point
**Category:** Numeric types
**DML:**
```
record
  decimal(",") sensor_id;
  real("4", ",") temperature;
  real("8", ",") pressure;
  string("\n") unit;
end;
```
**Expected:**
```python
StructType([
    StructField("sensor_id", LongType(), True),
    StructField("temperature", FloatType(), True),    # real(4) = 4-byte float
    StructField("pressure", DoubleType(), True),       # real(8) = 8-byte double
    StructField("unit", StringType(), True),
])
```
**Notes:** `real(4)` → `FloatType`, `real(8)` → `DoubleType`. Easy to confuse.

---

### TC-005: Void / skip fields
**Category:** Field skipping
**DML:**
```
record
  decimal(",") id;
  void(",") padding1;
  string(",") name;
  void(",") padding2;
  string("\n") status;
end;
```
**Expected:**
```python
# Skip void fields entirely in output schema
StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
])
# But read all columns then drop the void ones
```
**Notes:** `void` fields exist in source data but are dropped from output. Don't omit them when reading — only when projecting.

---

## 🟡 MEDIUM — Nullability, Dates, Defaults, Vectors

### TC-006: Nullable fields with null indicators
**Category:** Null handling
**DML:**
```
record
  decimal(",", null("")) customer_id;
  string(",", null("NULL")) middle_name;
  decimal("10.2", ",", null("-1")) discount;
  string("\n") status;
end;
```
**Expected:**
```python
schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("middle_name", StringType(), True),
    StructField("discount", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
])
# Apply per-field null replacement:
df = (spark.read.csv(path, schema=schema, nullValue="")  # global null
    .withColumn("middle_name", when(col("middle_name") == "NULL", None).otherwise(col("middle_name")))
    .withColumn("discount", when(col("discount") == -1, None).otherwise(col("discount"))))
```
**Notes:** Each field can have its own null sentinel. CSV reader's `nullValue` only handles one — others need post-read transformation.

---

### TC-007: Date and datetime types
**Category:** Temporal types, format conversion
**DML:**
```
record
  decimal(",") txn_id;
  date("YYYY-MM-DD")(",") txn_date;
  datetime("YYYYMMDDHH24MISS")(",") created_ts;
  datetime("YYYY-MM-DD HH24:MI:SS")("\n") updated_ts;
end;
```
**Expected:**
```python
# Read as strings, then convert:
StructType([
    StructField("txn_id", LongType(), True),
    StructField("txn_date", DateType(), True),
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
])
# Format mapping:
# YYYY-MM-DD -> yyyy-MM-dd
# YYYYMMDDHH24MISS -> yyyyMMddHHmmss
# HH24:MI:SS -> HH:mm:ss
df = df.withColumn("txn_date", to_date("txn_date", "yyyy-MM-dd")) \
       .withColumn("created_ts", to_timestamp("created_ts", "yyyyMMddHHmmss")) \
       .withColumn("updated_ts", to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"))
```
**Notes:** Format string conversion table:
- `YYYY` → `yyyy`
- `MM` → `MM` (same)
- `DD` → `dd`
- `HH24` → `HH`
- `HH12` → `hh`
- `MI` → `mm`
- `SS` → `ss`

---

### TC-008: Default values
**Category:** Defaults
**DML:**
```
record
  decimal(",") id;
  string(",") name = "UNKNOWN";
  decimal(",") qty = 0;
  string("\n") region = "GLOBAL";
end;
```
**Expected:**
```python
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("qty", LongType(), True),
    StructField("region", StringType(), True),
])
df = df.fillna({"name": "UNKNOWN", "qty": 0, "region": "GLOBAL"})
```
**Notes:** `fillna` with a dict is the cleanest mapping.

---

### TC-009: Fixed-length vectors of primitives
**Category:** Arrays
**DML:**
```
record
  decimal(",") student_id;
  decimal(",")[5] subject_scores;
  string(",")[3] favorite_subjects;
  string("\n") name;
end;
```
**Expected:**
```python
StructType([
    StructField("student_id", LongType(), True),
    StructField("subject_scores", ArrayType(LongType()), True),
    StructField("favorite_subjects", ArrayType(StringType()), True),
    StructField("name", StringType(), True),
])
# Read flat 9 columns (1 + 5 + 3 + name), then group:
df = df.withColumn("subject_scores", array("score_0","score_1","score_2","score_3","score_4")) \
       .withColumn("favorite_subjects", array("subj_0","subj_1","subj_2")) \
       .drop("score_0","score_1","score_2","score_3","score_4","subj_0","subj_1","subj_2")
```
**Notes:** CSV is positional. Read fields as flat columns first, then collapse into arrays.

---

### TC-010: Mixed delimiters
**Category:** Delimiter handling
**DML:**
```
record
  decimal("|") account_id;
  string("|") account_holder;
  decimal("8.2", "|") balance;
  date("YYYY-MM-DD")(";") opened_date;
  string("\n") branch;
end;
```
**Expected:**
- Flag this as **not directly supported** by `spark.read.csv` (it accepts only one delimiter).
- Strategy: read whole line as text, split on regex `[|;]`, or use a UDF.
```python
df = spark.read.text(path) \
    .select(split(col("value"), r"[|;\n]").alias("parts")) \
    .select(
        col("parts")[0].cast("long").alias("account_id"),
        col("parts")[1].alias("account_holder"),
        col("parts")[2].cast("decimal(8,2)").alias("balance"),
        to_date(col("parts")[3], "yyyy-MM-dd").alias("opened_date"),
        col("parts")[4].alias("branch"),
    )
```
**Notes:** Real Ab Initio jobs sometimes mix `|` and `;` — converter should detect and handle.

---

### TC-011: String with explicit length AND delimiter
**Category:** Hybrid string fields
**DML:**
```
record
  string(5, ",") product_code;
  string(50, ",") product_name;
  decimal("10.2", "\n") price;
end;
```
**Expected:**
```python
# string(5, ",") = up to 5 chars, terminated by ","
StructType([
    StructField("product_code", StringType(), True),  # truncate/pad to 5 if needed
    StructField("product_name", StringType(), True),  # up to 50
    StructField("price", DecimalType(10, 2), True),
])
# Optionally validate lengths post-read:
df = df.withColumn("product_code", substring("product_code", 1, 5))
```
**Notes:** Combined length+delimiter is common. Length is usually a max constraint, not strict.

---

### TC-012: Negative decimal indicators
**Category:** Numeric edge cases
**DML:**
```
record
  decimal(",") txn_id;
  decimal("10.2", ",") signed_amount;  // can be negative
  decimal("8", ",", null("0")) qty;
  string("\n") currency;
end;
```
**Expected:**
```python
StructType([
    StructField("txn_id", LongType(), True),
    StructField("signed_amount", DecimalType(10, 2), True),
    StructField("qty", LongType(), True),
    StructField("currency", StringType(), True),
])
df = df.withColumn("qty", when(col("qty") == 0, None).otherwise(col("qty")))
```
**Notes:** Decimal in DML is signed by default; just confirm Spark casts negatives correctly.

---

## 🔴 HARD — Nesting, Variable Vectors, Conditionals

### TC-013: Nested records
**Category:** Struct nesting
**DML:**
```
record
  decimal(",") customer_id;
  string(",") name;
  record
    string(",") street;
    string(",") city;
    string(",") state;
    string(",") zip;
  end address;
  string("\n") phone;
end;
```
**Expected:**
```python
StructType([
    StructField("customer_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True),
    StructField("phone", StringType(), True),
])
# Read flat, then build struct:
df = df.withColumn("address", struct("street", "city", "state", "zip")) \
       .drop("street", "city", "state", "zip") \
       .select("customer_id", "name", "address", "phone")
```
**Notes:** CSV is flat — converter must read flat then nest.

---

### TC-014: Deeply nested records
**Category:** Multi-level nesting
**DML:**
```
record
  decimal(",") id;
  record
    string(",") name;
    record
      string(",") street;
      record
        string(",") city;
        string(",") country;
      end location;
    end address;
  end customer;
  string("\n") status;
end;
```
**Expected:**
```python
StructType([
    StructField("id", LongType(), True),
    StructField("customer", StructType([
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("location", StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
            ]), True),
        ]), True),
    ]), True),
    StructField("status", StringType(), True),
])
```
**Notes:** Tests recursive descent in the parser. Build structs bottom-up.

---

### TC-015: Variable-length vectors (length-prefixed)
**Category:** Dynamic arrays
**DML:**
```
record
  decimal(",") order_id;
  decimal(",") item_count;
  string(",")[item_count] item_names;
  decimal(",")[item_count] item_quantities;
  string("\n") order_status;
end;
```
**Expected:**
```python
# Spark CSV can't natively handle this. Strategy: read raw line, split, slice.
StructType([
    StructField("order_id", LongType(), True),
    StructField("item_count", LongType(), True),
    StructField("item_names", ArrayType(StringType()), True),
    StructField("item_quantities", ArrayType(LongType()), True),
    StructField("order_status", StringType(), True),
])
# Pseudocode:
df = spark.read.text(path).select(split("value", ",").alias("p"))
df = (df
    .withColumn("order_id", col("p")[0].cast("long"))
    .withColumn("item_count", col("p")[1].cast("long"))
    .withColumn("item_names", expr("slice(p, 3, item_count)"))
    .withColumn("item_quantities",
        expr("transform(slice(p, 3 + item_count, item_count), x -> cast(x as long))"))
    .withColumn("order_status", expr("element_at(p, 3 + 2*item_count)"))
    .drop("p"))
```
**Notes:** Hardest part is positional slicing with a runtime length.

---

### TC-016: Vector of records
**Category:** Array of structs
**DML:**
```
record
  decimal(",") order_id;
  decimal(",") line_count;
  record[line_count]
    string(",") sku;
    decimal(",") qty;
    decimal("8.2", ",") price;
  end line_items;
  string("\n") status;
end;
```
**Expected:**
```python
StructType([
    StructField("order_id", LongType(), True),
    StructField("line_count", LongType(), True),
    StructField("line_items", ArrayType(StructType([
        StructField("sku", StringType(), True),
        StructField("qty", LongType(), True),
        StructField("price", DecimalType(8, 2), True),
    ])), True),
    StructField("status", StringType(), True),
])
# Read flat → group every 3 fields into struct → collect into array
```
**Notes:** Combine TC-013 nesting with TC-015 variable length.

---

### TC-017: Conditional records (if-then)
**Category:** Schema variants
**DML:**
```
record
  decimal(",") record_type;
  if (record_type == 1)
    record
      string(",") customer_name;
      decimal(",") customer_age;
    end customer_info;
  if (record_type == 2)
    record
      decimal(",") order_id;
      decimal("10.2", ",") order_amount;
    end order_info;
  string("\n") timestamp;
end;
```
**Expected:**
```python
# Strategy: union schema with all branches as nullable
StructType([
    StructField("record_type", LongType(), True),
    StructField("customer_info", StructType([
        StructField("customer_name", StringType(), True),
        StructField("customer_age", LongType(), True),
    ]), True),  # null when record_type != 1
    StructField("order_info", StructType([
        StructField("order_id", LongType(), True),
        StructField("order_amount", DecimalType(10, 2), True),
    ]), True),  # null when record_type != 2
    StructField("timestamp", StringType(), True),
])
# Population logic uses when().otherwise() per branch.
```
**Notes:** Document this as "schema with all branches; populate based on discriminator."

---

### TC-018: If-else chains
**Category:** Schema variants
**DML:**
```
record
  string(",") status_code;
  if (status_code == "A")
    string(",") active_reason;
  else if (status_code == "I")
    string(",") inactive_reason;
  else
    string(",") generic_reason;
  string("\n") updated_by;
end;
```
**Expected:**
```python
# Three nullable fields; populate by discriminator.
StructType([
    StructField("status_code", StringType(), True),
    StructField("active_reason", StringType(), True),
    StructField("inactive_reason", StringType(), True),
    StructField("generic_reason", StringType(), True),
    StructField("updated_by", StringType(), True),
])
```
**Notes:** Or alternative: collapse into a single `reason` field plus `status_code`. Document the choice.

---

### TC-019: Unions
**Category:** Polymorphic types
**DML:**
```
record
  decimal(",") entity_id;
  decimal(",") entity_type;
  union
    decimal(",") numeric_value;
    string(",") string_value;
    date("YYYY-MM-DD")(",") date_value;
  end value;
  string("\n") source_system;
end;
```
**Expected:**
```python
# Spark has no union. Map to struct of nullable branches:
StructType([
    StructField("entity_id", LongType(), True),
    StructField("entity_type", LongType(), True),
    StructField("value", StructType([
        StructField("numeric_value", LongType(), True),
        StructField("string_value", StringType(), True),
        StructField("date_value", DateType(), True),
    ]), True),
    StructField("source_system", StringType(), True),
])
```
**Notes:** Add a converter warning: "Union mapped to struct-of-nullables. Verify population logic downstream."

---

## ⚫ EXPERT — Mainframe, Includes, Real-World

### TC-020: Packed decimals (COMP-3)
**Category:** Mainframe binary types
**DML:**
```
record
  packed_decimal(5) account_num;
  packed_decimal("7.2") balance;
  zoned_decimal(4) status_code;
  string(20) account_name;
end;
```
**Expected:**
```python
# No native PySpark support. Generate UDF + read raw bytes.
# Mark as MANUAL_REVIEW in converter output.
schema = StructType([
    StructField("account_num", LongType(), True),
    StructField("balance", DecimalType(7, 2), True),
    StructField("status_code", LongType(), True),
    StructField("account_name", StringType(), True),
])
# Suggested UDF skeleton:
@udf(returnType=LongType())
def unpack_decimal(b: bytes) -> int:
    # nibble-level decode + sign nibble (C/D/F)
    ...
```
**Notes:** Converter should flag: "Packed/zoned decimals require custom byte-level decoding. Generated UDF stub — review before production use."

---

### TC-021: Include / external schema reference
**Category:** Modular schemas
**DML:**
```
include "common_address.dml";

record
  decimal(",") customer_id;
  string(",") name;
  address_t address;       // type defined in included file
  string("\n") phone;
end;
```
**common_address.dml:**
```
type address_t = record
  string(",") street;
  string(",") city;
  string(",") state;
  string(",") zip;
end;
```
**Expected:**
- Converter must resolve `include` and inline types before generating schema.
- Final output identical to TC-013.

**Notes:** Test the include resolver. Watch for circular includes.

---

### TC-022: Type aliases
**Category:** Named types
**DML:**
```
type money_t = decimal("12.2", ",");
type ts_t = datetime("YYYY-MM-DD HH24:MI:SS")(",");

record
  decimal(",") txn_id;
  money_t amount;
  money_t fee;
  ts_t created_at;
  string("\n") status;
end;
```
**Expected:**
```python
StructType([
    StructField("txn_id", LongType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("fee", DecimalType(12, 2), True),
    StructField("created_at", TimestampType(), True),
    StructField("status", StringType(), True),
])
```
**Notes:** Build a type symbol table during parse.

---

### TC-023: Comments and whitespace robustness
**Category:** Lexer robustness
**DML:**
```
/* Customer master record
   Last updated: 2024-Q3 */
record
    decimal(",") customer_id;     // primary key

    string(",")  name;            /* full name */
    // skip middle name for now
    decimal("10.2", ",") balance;
    string("\n") status;
end;
```
**Expected:** Same as a clean version — TC-001-style schema with `balance` as `DecimalType(10,2)`.
**Notes:** Lexer must strip `//`, `/* */`, and tolerate arbitrary whitespace/tabs.

---

### TC-024: EBCDIC encoding hint
**Category:** Charset
**DML:**
```
record
  string(10, charset="EBCDIC") emp_id;
  string(30, charset="EBCDIC") emp_name;
  packed_decimal(5) age;
end;
```
**Expected:**
```python
# Read raw bytes, decode EBCDIC explicitly:
df = (spark.read.format("binaryFile").load(path)
    .select(decode("content", "Cp037").alias("text"))  # Cp037 = US EBCDIC
    .select(
        substring("text", 1, 10).alias("emp_id"),
        substring("text", 11, 30).alias("emp_name"),
        # packed_decimal needs UDF
    ))
```
**Notes:** Flag for review. Charset detection is environment-specific.

---

### TC-025: Real-world combo (the boss fight)
**Category:** Everything at once
**DML:**
```
record
  decimal(",") txn_id;
  datetime("YYYY-MM-DD HH24:MI:SS")(",") txn_timestamp;
  decimal(",") customer_id;
  decimal(",") txn_type;  // 1=purchase, 2=refund

  record
    string(",", null("")) merchant_name;
    string(",") merchant_category;
    decimal("10.2", ",") amount;
  end merchant_info;

  decimal(",") item_count;
  record[item_count]
    string(",") sku;
    decimal(",") quantity;
    decimal("8.2", ",") line_total;
  end line_items;

  if (txn_type == 2)
    record
      decimal(",") original_txn_id;
      string(",") refund_reason;
    end refund_details;

  string("\n", null("UNKNOWN")) channel;
end;
```
**Expected:** Full schema combining nested records, vector-of-records with runtime length, conditional sub-record, multiple null sentinels, datetime conversion, decimal precision. See individual tests above for each piece.

**Notes:** If TC-001 through TC-024 pass and TC-025 passes, the converter handles ~95% of real-world DML.

---

## Test Harness Suggestion

Organize tests as:
```
tests/
├── fixtures/
│   ├── tc_001/
│   │   ├── input.dml
│   │   ├── expected_schema.json    # serialized StructType
│   │   ├── sample_data.csv          # tiny sample
│   │   └── expected_output.json     # expected DataFrame rows
│   ├── tc_002/
│   └── ...
├── test_schema_conversion.py        # asserts StructType equality
├── test_read_logic.py               # asserts DataFrame rows match
└── conftest.py
```

For each test, assert:
1. **Schema equality** — generated `StructType` matches expected JSON
2. **Read correctness** — running generated PySpark on `sample_data.csv` produces `expected_output.json`
3. **Warnings** — for hard cases (TC-019, TC-020, TC-024), assert a warning was emitted

---

## Coverage Matrix

| Feature                       | Test Cases                          |
|-------------------------------|-------------------------------------|
| Primitive types               | TC-001, TC-003, TC-004              |
| Fixed-length parsing          | TC-002, TC-024                      |
| Delimited parsing             | TC-001, TC-010, TC-011              |
| Null handling                 | TC-006, TC-012, TC-025              |
| Date/time formats             | TC-007, TC-022, TC-025              |
| Defaults                      | TC-008                              |
| Void/skip fields              | TC-005                              |
| Fixed-length vectors          | TC-009                              |
| Variable-length vectors       | TC-015, TC-016, TC-025              |
| Nested records                | TC-013, TC-014, TC-021, TC-025      |
| Conditional records           | TC-017, TC-018, TC-025              |
| Unions                        | TC-019                              |
| Includes / type aliases       | TC-021, TC-022                      |
| Mainframe (packed/EBCDIC)     | TC-020, TC-024                      |
| Lexer robustness              | TC-023                              |
| Mixed delimiters              | TC-010                              |
