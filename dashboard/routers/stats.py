from fastapi import APIRouter, Request
from sqlalchemy import inspect, text

from src.config import MONGO_DB_NAME


router = APIRouter()


@router.get("/api/stats")
async def get_stats(request: Request):
    if getattr(request.app.state, "pipeline_busy", False):
        return {"status": "pipeline_busy"}

    sql_payload = {"reachable": False, "tables": [], "error": None}
    mongo_payload = {"reachable": False, "collections": [], "error": None}

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
        else:
            sql_payload["error"] = "SQL engine is not initialized"
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

    return {
        "status": "ok",
        "sql": sql_payload,
        "mongo": mongo_payload,
    }
