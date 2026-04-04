# A3 Script Summary

This document covers the Python scripts introduced for Assignment 3 or used as the A3 dashboard, ACID, and transactional-validation layer. Files that predate A3 but are still reused are listed separately at the end for context.

## Dashboard Runtime

| File | Purpose | Summary |
|---|---|---|
| `project_config.py` | Shared runtime config | Centralizes defaults for the dashboard and pipeline, including API host/port, startup timeouts, record counts, and Docker container names. |
| `dashboard/run.py` | Dashboard entrypoint | Starts the FastAPI dashboard with Uvicorn on port 8080 and ensures the project root is on `sys.path`. |
| `dashboard/app.py` | FastAPI app bootstrap | Creates the dashboard app, initializes shared resources during lifespan startup, mounts static files, loads templates, and registers all routers. |
| `dashboard/dependencies.py` | Dependency helpers | Provides small accessors for retrieving shared objects such as the SQL engine, MongoDB client, and transaction coordinator from `app.state`. |
| `dashboard/routers/pipeline.py` | Pipeline API router | Exposes endpoints for saving the schema, running the pipeline initialise flow, and triggering fetch/incremental ingest operations. |
| `dashboard/routers/query.py` | Query API router | Validates query payloads and routes CRUD-style logical queries into the hybrid storage layer. |
| `dashboard/routers/stats.py` | Status and health router | Returns dashboard health information such as backend connectivity and runtime status summaries. |
| `dashboard/routers/acid.py` | ACID API router | Exposes the ACID validation endpoints used by the dashboard to run basic, advanced, or full test suites. |

## ACID Validation

| File | Purpose | Summary |
|---|---|---|
| `ACID/validators.py` | Basic ACID checks | Implements the core atomicity, consistency, isolation, and durability tests against the hybrid database state. |
| `ACID/advanced_validators.py` | Extended ACID checks | Adds deeper scenarios such as multi-record atomicity, cross-database atomicity, dirty-read prevention, concurrency stress tests, and persistence checks. |
| `ACID/runner.py` | ACID test dispatcher | Provides a simple API for running one ACID test, all basic tests, one advanced test, or all advanced tests; also keeps a CLI entrypoint for terminal use. |

## Hybrid Storage Engines

| File | Purpose | Summary |
|---|---|---|
| `src/phase_5/sql_schema_definer.py` | SQL schema builder | Reads metadata and builds SQLAlchemy models and tables for the relational side of the hybrid store. |
| `src/phase_5/sql_engine.py` | SQL engine | Handles normalization, inserts, queries, and other PostgreSQL-side operations for records routed to SQL. |
| `src/phase_5/sql_pipeline.py` | SQL ingestion pipeline | Coordinates SQL schema setup, bulk loading, and archival of successfully processed SQL records. |
| `src/phase_5/mongo_engine.py` | MongoDB engine | Translates routed records into MongoDB documents and handles the MongoDB-side persistence logic. |

## Transactional Query Layer

| File | Purpose | Summary |
|---|---|---|
| `src/phase_6/CRUD_json_reader.py` | Query payload validator | Validates CRUD query JSON structure and can still be used as a terminal-style reader when needed. |
| `src/phase_6/CRUD_runner.py` | Query orchestration | Parses logical queries, determines which databases are involved, and dispatches to the CRUD operations layer. |
| `src/phase_6/CRUD_operations.py` | CRUD implementation | Implements create, read, update, and delete logic across SQL, MongoDB, and unknown-record handling. |
| `src/phase_6/transaction_coordinator.py` | Saga coordinator | Manages multi-step transactional execution with compensation and logging for cross-database safety. |

## A2-Reused Support Files Referenced By A3

These files are not new A3 additions, but A3 still depends on them:

| File | Role in A3 |
|---|---|
| `main.py` | Core pipeline entrypoint reused by the dashboard pipeline router for initialise and fetch flows. |
| `external/app.py` | Data-generation API that feeds the ingestion pipeline. |
| `starter.py` | Legacy Docker lifecycle helper retained for the original CLI workflow. |
| `src/config.py` | Shared path and environment configuration used by the pipeline and storage layers. |

## Quick Read

- Dashboard code lives under `dashboard/` and is the user-facing A3 layer.
- ACID validation code lives under `ACID/` and is exposed through dashboard routes and CLI helpers.
- `src/phase_5/` and `src/phase_6/` contain the hybrid storage and transactional query logic that power the dashboard.
- The older pipeline entrypoints remain available, but they are support code rather than the main A3 surface.
