from fastapi import Request

from src.config import MONGO_DB_NAME


def get_sql_engine(request: Request):
    return request.app.state.sql_engine


def get_mongo_db(request: Request):
    return request.app.state.mongo_client[MONGO_DB_NAME]


def get_coordinator(request: Request):
    return request.app.state.coordinator
