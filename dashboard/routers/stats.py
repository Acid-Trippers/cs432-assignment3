from fastapi import APIRouter, Request
import httpx
from sqlalchemy import inspect, text
from pathlib import Path

from src.config import API_HOST, INITIAL_SCHEMA_FILE, METADATA_FILE, MONGO_DB_NAME


router = APIRouter()


@router.get("/api/status")
async def get_status(request: Request):
    # Use config file presence as source of truth for resumability.
    has_schema = Path(INITIAL_SCHEMA_FILE).exists()
    has_metadata = Path(METADATA_FILE).exists()

    return {
        "pipeline_state": getattr(request.app.state, "pipeline_state", "fresh"),
        "has_schema": has_schema,
        "has_metadata": has_metadata,
        "pipeline_busy": getattr(request.app.state, "pipeline_busy", False),
    }


@router.get("/api/stats")
async def get_stats(request: Request):
    if getattr(request.app.state, "pipeline_busy", False):
        return {"status": "pipeline_busy"}

    sql_payload = {"reachable": False, "tables": [], "error": None}
    mongo_payload = {"reachable": False, "collections": [], "error": None}

    if not getattr(request.app.state, "sql_initialized", False):
        sql_payload = {
            "reachable": False,
            "tables": [],
            "reason": "Pipeline not initialized yet",
            "error": None,
        }
    else:
        # SQL stats with short-lived connection from the shared SQL engine.
        try:
            sql_engine = request.app.state.sql_engine
            raw_engine = sql_engine.schema_builder.engine

            if raw_engine is not None:
                inspector = inspect(raw_engine)
                table_names = inspector.get_table_names()

                with raw_engine.connect() as conn:
                    tables = []
                    for table_name in table_names:
                        safe_name = table_name.replace('"', '""')
                        count_stmt = text(f'SELECT COUNT(*) FROM "{safe_name}"')
                        row_count = conn.execute(count_stmt).scalar_one()
                        tables.append({"name": table_name, "rows": row_count})

                sql_payload = {"reachable": True, "tables": tables, "error": None}
        except Exception as exc:
            sql_payload["error"] = str(exc)

    # Mongo stats using the shared Mongo client.
    try:
        mongo_db = request.app.state.mongo_client[MONGO_DB_NAME]
        collection_names = mongo_db.list_collection_names()
        collections = [
            {"name": name, "documents": mongo_db[name].count_documents({})}
            for name in collection_names
        ]
        mongo_payload = {"reachable": True, "collections": collections, "error": None}
    except Exception as exc:
        mongo_payload["error"] = str(exc)

    external_api_reachable = False
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{API_HOST}/")
            external_api_reachable = response.status_code == 200
    except Exception:
        external_api_reachable = False

    return {
        "status": "ok",
        "sql": sql_payload,
        "mongo": mongo_payload,
        "external_api_reachable": external_api_reachable,
    }
