from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient

from dashboard.routers import acid, pipeline, query, stats
from src.config import MONGO_URI, TRANSACTION_LOG_FILE
from src.phase_5.sql_engine import SQLEngine
from src.phase_6.transaction_coordinator import TransactionCoordinator


BASE_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    sql_engine = SQLEngine()
    sql_engine.initialize()

    mongo_client = MongoClient(MONGO_URI)

    coordinator = TransactionCoordinator(log_file=TRANSACTION_LOG_FILE)

    app.state.sql_engine = sql_engine
    app.state.mongo_client = mongo_client
    app.state.coordinator = coordinator
    app.state.pipeline_busy = False

    try:
        yield
    finally:
        try:
            app.state.sql_engine.close()
        except Exception:
            pass

        try:
            app.state.mongo_client.close()
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.state.templates = templates

app.include_router(stats.router)
app.include_router(pipeline.router)
app.include_router(query.router)
app.include_router(acid.router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
