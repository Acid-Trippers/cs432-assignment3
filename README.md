# CS432 Assignment 3 Dashboard

_(CS432 - Databases Assignment 3: Logical Dashboard & Transactional Validation)_

This repository exposes the hybrid SQL + MongoDB system through a FastAPI dashboard. The dashboard presents logical entities, query results, and ACID validation output without showing backend-specific storage details.

Demo video: add your submission link here.

## What This Project Includes

- A logical dashboard at the application root.
- Dashboard APIs for running ACID validation tests.
- The underlying hybrid database engine from Assignment 2, reused as the data layer.

## How To Run The Dashboard

1. Install dependencies.

```powershell
pip install -r requirements.txt
```

2. Start backend services first (required before pipeline endpoints).

```powershell
docker-compose up -d
```

This starts PostgreSQL, MongoDB, and the external ingestion API. The pipeline endpoints (`/api/pipeline/schema`, `/api/pipeline/initialise`, `/api/pipeline/fetch`) depend on those services being reachable.

3. Start the dashboard server.

```powershell
.venv/bin/python dashboard/run.py
```

If you are using an activated virtual environment, `python dashboard/run.py` works as well.

4. Open the dashboard in your browser.

```text
http://127.0.0.1:8080/
```

If the external API is unreachable, the dashboard displays a warning banner. The same signal is available in `GET /api/stats` via `external_api_reachable`.

## ACID Test Endpoints

The dashboard also exposes ACID validation routes under `/api/acid`:

- `GET /api/acid/all` runs the full ACID suite.
- `GET /api/acid/{test_name}` runs a single basic ACID test such as `atomicity` or `consistency`.
- `GET /api/acid/advanced/{test_name}` runs a single advanced validation test.

## Project Structure

```text
cs432-assignment3/
├── ACID/               # ACID validation runners and validators
├── dashboard/          # FastAPI app, templates, static assets, and routes
├── data/               # Pipeline and transaction artifacts
├── docs/               # Assignment and architecture documentation
├── src/                # Hybrid SQL + MongoDB engine
├── main.py             # Core pipeline entrypoint
├── starter.py          # Environment bootstrapper for the backend stack
└── requirements.txt    # Python dependencies
```

## Documentation

- [Assignment 3 Guidelines](docs/assignment-3-guidelines.md)
- [ACID Testing Report](ACID/TEST_REPORT.md)
- [SQL Engine Architecture](docs/SQL_ENGINE_ARCHITECTURE.md)

## Notes

The dashboard is served by `dashboard/run.py`, which launches Uvicorn on port `8080` and loads the FastAPI app defined in `dashboard/app.py`.
