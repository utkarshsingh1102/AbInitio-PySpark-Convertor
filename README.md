# IBM Network — Ab Initio → PySpark Convertor

Converts Ab Initio graphs into runnable PySpark code. This repo implements the **IBM Network** half of the architecture (Mapping + DML convertor + Code generator). The Parser (Client Network) is a separate workstream and emits its output to Neo4j.

## Quick start

```bash
make dev                  # install with dev extras
make infra-up             # start Neo4j + Ollama via docker compose
make test                 # run unit tests (no infra required)
```

## Web UI

A local browser tool for exploring DML schemas and read code without running the full pipeline.

```bash
pip install -e ".[web]"
uvicorn ibm_network.web.server:app --reload --port 8000
open http://localhost:8000
```

### Tabs

| Tab | What it does |
|---|---|
| **DML schema** | Paste a DML record → get the PySpark `StructType` schema **and** the full `spark.read` chain (with per-field null replacement, date parsing, and vector/struct assembly). |
| **Transform body** | Paste an Ab Initio `out.x :: expr; ...` block → get the equivalent `df.select(...)` arguments. |
| **Single expression** | Paste one Ab Initio transform expression → get the PySpark `Column` expression. |
| **Full pipeline** | POST a graph JSON → get a complete runnable PySpark `.py` file. |

The **DML schema** tab output is formatted with one method call per line for readability:

```python
# Schema
schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("middle_name", StringType(), True),
    StructField("discount", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
])

# Read
df = (
    spark
    .read
    .option("header", "false")
    .csv("<input_path>", schema=schema)
    .withColumn("middle_name", F.when(F.col("middle_name") == F.lit("NULL"), None).otherwise(F.col("middle_name")))
    .withColumn("discount", F.when(F.col("discount") == F.lit(-1), None).otherwise(F.col("discount")))
)
```

## Layout

```
ibm_network/
  ir/              IR models + Neo4j loader (Parser handoff)
  dml/             DML record-format → PySpark StructType
  mapping/         Ab Initio component → PySpark op (rule-based + LLM fallback)
  codegen/         Synthesizer + Jinja templates + local-LLM client
  config/          settings + declarative component map
  web/             FastAPI server + static UI (server.py, static/)
  cli.py           `ibm-net convert <graph_name> --out <dir>`
tests/             unit + integration tests
```

## DML support

| Feature | Status |
|---|---|
| Scalar types: `decimal`, `integer`, `real`, `string`, `date`, `datetime`, `void` | ✅ |
| Delimited fields, per-field null sentinels | ✅ |
| Fixed-width (no-delimiter) records | ✅ |
| Nested sub-records (`record ... end name;`) | ✅ |
| Fixed-length vectors (`field[N]`) | ✅ |
| Variable-length vectors (`record[disc] ... end name;`) | ✅ |
| Union fields (`union ... end name;`) | ✅ (MANUAL_REVIEW flagged) |
| Conditional fields (`if (...) field else if (...) field else field`) | ✅ |
| Packed / zoned decimal | ✅ (MANUAL_REVIEW flagged) |
| Mixed-delimiter records | ✅ |

## Running the full pipeline

```bash
docker compose up -d neo4j ollama
ollama pull qwen2.5-coder:14b                       # one-time
cypher-shell -u neo4j -p devpassword \
    -f tests/fixtures/graphs/sample_01.cypher       # seed Neo4j
ibm-net convert sample_01 --out out/                # run pipeline
spark-submit out/sample_01.py                       # confirm runnable
```

## Status

**90 / 99 test cases passing.**

v1 covers six components — `REFORMAT`, `FILTER_BY_EXPRESSION`, `JOIN`, `SORT`, `ROLLUP`, `DEDUP_SORTED` — and the full DML type surface including nested records, vectors, unions, and conditional fields.

Remaining open items: include resolver (`%include`), type aliases (`typedef`), EBCDIC charset attribute, and the boss-fight integration test (TC-025).
