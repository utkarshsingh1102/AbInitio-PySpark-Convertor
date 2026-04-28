"""FastAPI server exposing the convertor as a web UI.

Run locally:
    pip install -e ".[web]"
    python -m uvicorn ibm_network.web.server:app --reload --port 8000
    open http://localhost:8000
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ibm_network.codegen import synthesize
from ibm_network.dml.emitter import render_schema_from_text
from ibm_network.ir.models import Graph
from ibm_network.mapping.transform_expr import expr_to_pyspark, transform_block_to_select_args

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="IBM Network — Ab Initio → PySpark")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class TextRequest(BaseModel):
    text: str


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/dml")
def api_dml(req: TextRequest) -> dict:
    try:
        struct_src = render_schema_from_text(req.text)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"DML parse error: {e}") from e
    py_file = (
        "from pyspark.sql.types import (\n"
        "    StructType, StructField, DecimalType, IntegerType, LongType,\n"
        "    ShortType, ByteType, StringType, DateType, TimestampType,\n"
        ")\n\n"
        f"schema = {struct_src}\n"
    )
    return {"schema_source": struct_src, "py_file": py_file}


@app.post("/api/transform")
def api_transform(req: TextRequest) -> dict:
    try:
        args = transform_block_to_select_args(req.text)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"transform parse error: {e}") from e
    body = ",\n".join(f"        {a}" for a in args)
    py_file = (
        "from pyspark.sql import functions as F\n\n\n"
        "def reshape(df):\n"
        "    return df.select(\n"
        f"{body}\n"
        "    )\n"
    )
    return {"select_args": args, "py_file": py_file}


@app.post("/api/expr")
def api_expr(req: TextRequest) -> dict:
    try:
        src = expr_to_pyspark(req.text)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"expr parse error: {e}") from e
    return {"column_source": src}


@app.post("/api/pipeline")
def api_pipeline(graph_dict: dict) -> dict:
    try:
        graph = Graph.model_validate(graph_dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid graph spec: {e}") from e
    try:
        result = synthesize(graph, enable_llm_fallback=False, enable_llm_polish=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"synthesis failed: {e}") from e
    return {"code": result.code, "notes": result.notes, "final_var": result.final_var}
