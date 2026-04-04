import asyncio
import importlib
import json

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from src.config import INITIAL_SCHEMA_FILE
from src.phase_5.sql_engine import SQLEngine


router = APIRouter()


class SchemaPayload(BaseModel):
    schema: dict


def _dispose_shared_sql_engine(request: Request) -> None:
    sql_engine = request.app.state.sql_engine

    try:
        sql_engine.close()
    except Exception:
        pass

    try:
        raw_engine = sql_engine.schema_builder.engine
        if raw_engine is not None:
            raw_engine.dispose()
    except Exception:
        pass


def _reinitialize_shared_sql_engine(request: Request) -> None:
    new_engine = SQLEngine()
    if not new_engine.initialize():
        raise RuntimeError("Failed to reinitialize shared SQL engine")
    request.app.state.sql_engine = new_engine


@router.post("/api/pipeline/schema")
async def save_schema(payload: SchemaPayload):
    try:
        schema_module = importlib.import_module("src.phase_1_to_4.00_schema_definition")
        schema_module.validate_structure(payload.schema)

        with open(INITIAL_SCHEMA_FILE, "w", encoding="utf-8") as schema_file:
            json.dump(payload.schema, schema_file, indent=4)

        return {
            "valid": True,
            "saved_to": INITIAL_SCHEMA_FILE,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/pipeline/initialise")
async def run_initialise(request: Request, count: int = Query(default=1000, ge=0)):
    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is already running")

    request.app.state.pipeline_busy = True

    try:
        _dispose_shared_sql_engine(request)

        main_module = importlib.import_module("main")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: main_module.initialise(count))

        _reinitialize_shared_sql_engine(request)

        return {
            "status": "completed",
            "operation": "initialise",
            "count": count,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        request.app.state.pipeline_busy = False


@router.post("/api/pipeline/fetch")
async def run_fetch(request: Request, count: int = Query(default=100, ge=0)):
    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is already running")

    request.app.state.pipeline_busy = True

    try:
        _dispose_shared_sql_engine(request)

        main_module = importlib.import_module("main")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: main_module.fetch(count))

        _reinitialize_shared_sql_engine(request)

        return {
            "status": "completed",
            "operation": "fetch",
            "count": count,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        request.app.state.pipeline_busy = False
