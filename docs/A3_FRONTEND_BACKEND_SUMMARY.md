# Assignment 3 Frontend and Backend Summary

This document summarizes the frontend and backend work created so far for the Assignment 3 dashboard layer.

## Overall Goal

The project exposes the hybrid SQL + MongoDB system through a FastAPI dashboard that keeps the user focused on logical workflow rather than storage internals. The UI supports schema setup, pipeline control, query execution, statistics viewing, and ACID validation.

## Backend Structure

### Application bootstrap
- [dashboard/app.py](../dashboard/app.py) creates the FastAPI app, mounts static assets, registers templates, and wires the routers.
- The app lifespan initializes shared runtime state:
  - SQL engine
  - MongoDB client
  - transaction coordinator
  - pipeline status flags such as `pipeline_state`, `sql_initialized`, and `pipeline_busy`
- The app also checks for existing schema/metadata files so it can resume in the correct state after restart.

### Server entrypoint
- [dashboard/run.py](../dashboard/run.py) is the dashboard launcher.
- It runs Uvicorn on port `8080` and points to `dashboard.app:app`.

### API routes
- [dashboard/routers/pipeline.py](../dashboard/routers/pipeline.py)
  - `POST /api/pipeline/schema` saves the user schema to `data/initial_schema.json`.
  - `POST /api/pipeline/initialise` runs the main initialization flow.
  - `POST /api/pipeline/fetch` runs incremental fetch/ingestion.
  - `POST /api/pipeline/reset` clears runtime state and data artifacts while preserving schema when needed.
- [dashboard/routers/query.py](../dashboard/routers/query.py)
  - `POST /api/query` validates CRUD-style payloads and dispatches them to the transactional query layer.
- [dashboard/routers/stats.py](../dashboard/routers/stats.py)
  - `GET /api/status` returns pipeline state and resumability flags.
  - `GET /api/stats` reports SQL, MongoDB, and external API reachability information.
- [dashboard/routers/acid.py](../dashboard/routers/acid.py)
  - `GET /api/acid/all` runs the full ACID suite.
  - `GET /api/acid/{test_name}` runs one basic ACID test.
  - `GET /api/acid/advanced/{test_name}` runs one advanced ACID test.

### Shared backend behavior
- The backend uses shared app state instead of rebuilding resources for every request.
- Pipeline operations guard against concurrent execution with `pipeline_busy`.
- SQL resources are closed and reinitialized around pipeline operations to keep the dashboard aligned with the data layer.
- The stats endpoint hides backend internals from the UI and reports only the logical health indicators needed by the dashboard.

## Frontend Structure

### Landing page
- [dashboard/templates/index.html](../dashboard/templates/index.html) is the entry screen.
- It shows the current pipeline state and exposes the main control points:
  - set up schema
  - run initialise
  - continue to dashboard
  - reset everything
- The page also surfaces an external API warning when backend services are unavailable.

### Schema setup page
- [dashboard/templates/setup.html](../dashboard/templates/setup.html) is used before initialization.
- It lets the user view or edit the JSON schema, save it, and then run the initialization step.
- The schema payload is prefilled from the saved schema file when available.

### Initialized dashboard
- [dashboard/templates/dashboard.html](../dashboard/templates/dashboard.html) is the main working view after initialization.
- It contains four major sections:
  - fetch controls for incremental ingestion
  - stats panel for SQL and MongoDB status
  - query interface for logical CRUD operations
  - ACID test area for validation runs

### Styling
- [dashboard/static/style.css](../dashboard/static/style.css) defines the visual language for all pages.
- The design uses:
  - warm gradients and card surfaces
  - green-accented action states
  - responsive grid layouts
  - compact status pills and badges
  - distinct card treatments for dashboard, query, and ACID sections
- The layout adapts down to mobile widths with single-column stacking.

### Client-side logic
- [dashboard/static/main.js](../dashboard/static/main.js) drives all page interactions.
- It detects which page is loaded and attaches the correct handlers.
- It handles:
  - fetching `/api/status` and `/api/stats`
  - switching visible controls based on pipeline state
  - showing and hiding feedback messages
  - starting initialise, fetch, and reset actions
  - saving schemas from the setup page
  - running logical queries and rendering results
  - executing ACID tests and rendering pass/fail badges
  - downloading query results as JSON

## Frontend Workflows

### 1. Schema setup and initialization
1. The user opens the landing page.
2. If the pipeline is fresh, the UI exposes the schema setup path.
3. The user edits the JSON schema on the setup page and saves it through `/api/pipeline/schema`.
4. The user runs initialization through `/api/pipeline/initialise`.
5. Once initialization completes, the dashboard becomes available.

### 2. Dashboard inspection
1. The dashboard shows SQL and MongoDB connectivity.
2. It reports record counts without exposing storage implementation details.
3. It refreshes status after fetch or reset actions.

### 3. Query execution
1. The user selects a CRUD operation.
2. The client loads a template JSON payload into the editor.
3. The user edits the logical query.
4. The payload is sent to `/api/query`.
5. Results are rendered either as a table for reads or a summary view for write operations.

### 4. ACID validation
1. The user runs one basic test or the full suite.
2. Results are shown with pass/fail badges.
3. Raw JSON is available under each test card for inspection.

## Data And Runtime Dependencies

- The pipeline reads and writes files under [data/](../data/).
- Schema setup persists to `data/initial_schema.json`.
- Runtime reset clears transient artifacts while preserving the schema when appropriate.
- The dashboard expects the external ingestion API, PostgreSQL, and MongoDB to be available for full functionality.

## What Exists So Far

The current frontend/backend work provides:
- a landing page for controlling pipeline state
- a schema editor and initialization flow
- a logical dashboard for stats, fetches, queries, and ACID checks
- API endpoints that connect the UI to the underlying hybrid storage engine
- a shared runtime model that keeps the dashboard synchronized with backend state

## Files At A Glance

- [dashboard/app.py](../dashboard/app.py)
- [dashboard/run.py](../dashboard/run.py)
- [dashboard/routers/pipeline.py](../dashboard/routers/pipeline.py)
- [dashboard/routers/query.py](../dashboard/routers/query.py)
- [dashboard/routers/stats.py](../dashboard/routers/stats.py)
- [dashboard/routers/acid.py](../dashboard/routers/acid.py)
- [dashboard/templates/index.html](../dashboard/templates/index.html)
- [dashboard/templates/setup.html](../dashboard/templates/setup.html)
- [dashboard/templates/dashboard.html](../dashboard/templates/dashboard.html)
- [dashboard/static/main.js](../dashboard/static/main.js)
- [dashboard/static/style.css](../dashboard/static/style.css)
