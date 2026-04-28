# IBM Network — Ab Initio → PySpark Convertor

Converts Ab Initio graphs into runnable PySpark code. This repo implements the **IBM Network** half of the architecture (Mapping + DML convertor + Code generator). The Parser (Client Network) is a separate workstream and emits its output to Neo4j.

## Quick start

```bash
make dev                  # install with dev extras
make infra-up             # start Neo4j + Ollama via docker compose
make test                 # run unit tests (no infra required)
```

## Layout

```
ibm_network/
  ir/              IR models + Neo4j loader (Parser handoff)
  dml/             DML record-format → PySpark StructType
  mapping/         Ab Initio component → PySpark op (rule-based + LLM fallback)
  codegen/         Synthesizer + Jinja templates + local-LLM client
  config/          settings + declarative component map
  cli.py           `ibm-net convert <graph_name> --out <dir>`
tests/             unit + integration tests
```

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

v1 covers six components — `REFORMAT`, `FILTER_BY_EXPRESSION`, `JOIN`, `SORT`, `ROLLUP`, `DEDUP_SORTED` — and the DML scalar primitives.
