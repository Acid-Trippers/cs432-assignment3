from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


router = APIRouter()


class QueryPayload(BaseModel):
    operation: str
    entity: str
    filters: Dict[str, Any] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(default_factory=dict)


@router.post("/api/query")
async def execute_query(body: QueryPayload):
    try:
        from src.phase_6.CRUD_runner import query_runner

        result = query_runner(query_dict=body.model_dump())
        if result is None:
            raise HTTPException(status_code=400, detail="query could not be executed")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
