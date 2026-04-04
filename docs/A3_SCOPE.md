# A3 File Scope Categorization

**Purpose:** This document categorizes every Python file in the A2 codebase into three buckets—Read-only (core business logic), Modifiable (needs wiring for web), and Scrapped (replaced by dashboard). Use this before any agent modification to avoid touching files outside scope.

**Evaluation Criteria:**
1. **CLI Assumptions** — does it use `input()`, `sys.exit()`, `print()` for primary output, or `argparse`? If yes → hostile to web workers → needs modification or scrapping.
2. **Import-Time Side Effects** — does it open DB connections, read files, or run heavy initialization at module level (outside functions)? If yes → breaks FastAPI lifespan model → needs modification.
3. **Orchestration vs. Business Logic** — is it purely routing/routing between other modules? If yes + CLI-oriented → candidate for scrapping (FastAPI routers take over). If it contains reusable logic → keep but rewire.

---

## READ-ONLY — Core Business Logic (Never Modify)

These files contain reusable algorithms with no CLI assumptions or import-time side effects. Agents will call these modules from FastAPI endpoints.

| File | Purpose | Why Read-Only |
|------|---------|--------------|
| `project_config.py` | Configuration constants (ports, timeouts, etc.) | Pure data constants; no I/O or CLI assumptions |
| `src/config.py` | Canonical paths and env vars (DATABASE_URL, FILE_PATHS) | Reads env vars only (acceptable); side effect is harmless `makedirs()` |
| `src/phase_1_to_4/01_ingestion.py` | Fetch raw records from API via async HTTP, parse Server-Sent Events | Async I/O is non-blocking; no CLI assumptions; pure business logic ✓ |
| `src/phase_1_to_4/02_cleaner.py` | Normalize records to schema, map/cast fields, quarantine unmapped | Pure algorithmic; no print(), no blocking calls ✓ |
| `src/phase_1_to_4/03_analyzer.py` | Profile cleaned records: frequency, type stability, cardinality | Statistical analysis only; no I/O beyond JSON reads (pre-loaded) ✓ |
| `src/phase_1_to_4/04_metadata_builder.py` | Merge schema constraints with analysis; flag probation fields | Pure merge logic; reads from pre-loaded JSON dicts ✓ |
| `src/phase_1_to_4/05_classifier.py` | Route fields to SQL/MONGO/UNKNOWN based on heuristics | Pure decision logic; no print/CLI assumptions in core algorithm ✓ |
| `src/phase_1_to_4/06_router.py` | Horizontally partition records into SQL/MongoDB/Unknown shards | Routing algorithm only; atomic file writes are safe ✓ |
| `src/phase_5/sql_schema_definer.py` | Analyze metadata, build SQLAlchemy ORM models, create tables | Pure schema builder; no print/CLI assumptions; initialization safe to call from FastAPI lifespan ✓ |
| `src/phase_5/sql_engine.py` | SQL CRUD operations, bulk insert, query, normalize records | Pure business logic; connections lazily-initialized in `initialize()` method ✓ |
| `src/phase_6/transaction_coordinator.py` | Saga-style distributed transactions with compensation & logging | Pure orchestration logic; thread-safe with locks ✓ Can be used directly from web workers |

---

## MODIFIABLE — Files Needing Wiring/Extension for A3

These files contain business logic mixed with CLI assumptions or import-time side effects. Agents must separate concerns: keep business logic, move CLI/blocking calls to FastAPI.

### ORCHESTRATION/ENTRY POINTS (Prioritize First)

| File | Issue | What A3 Must Do |
|------|-------|-----------------|
| `main.py` | • Uses `argparse`, `sys.argv` for CLI dispatch<br/>• Spawns `external/app.py` as subprocess (CLI assumption)<br/>• Calls `wait_for_api()` with blocking TCP polls<br/>• Prints progress to stdout<br/>• Calls `sys.exit()` on error | **DELETE:** subprocess spawning (lines 373–376), wait_for_api() (lines 377–382)<br/>**EXTRACT:** Keep `initialise()`, `fetch()`, `query()` as importable functions (no argparse)<br/>**MOVE:** CLI dispatch to FastAPI route handlers (not in main.py)<br/>**KEEP:** Checkpoint logic, error handling (adapt for async) |
| `external/app.py` | • Standalone FastAPI server on port 8000<br/>• Assumes CLI invocation via `python external/app.py`<br/>• Not integrated with dashboard | **INTEGRATE:** Merge endpoints into main dashboard FastAPI app<br/>**REMOVE:** Standalone `uvicorn.run()` invocation<br/>**KEEP:** Data generation logic (`generate_record()`, stream handlers) |
| `starter.py` | • CLI for Docker lifecycle: `python starter.py start`/`end`<br/>• `sys.argv` parsing, subprocess spawning (docker-compose)<br/>• Windows hardcoded Docker path (won't work on Mac/Linux)<br/>• Blocking polls for port readiness<br/>• Assumes terminal available | **DEPRECATE FOR A3:** FastAPI runs in container; Docker Compose managed by deployment (K8s/Docker Compose spec, not CLI)<br/>**DO NOT USE:** This file will break in web context<br/>**ALTERNATIVE:** Ops team runs `docker-compose up` or K8s manifest; dashboard assumes infrastructure is ready |

### SCHEMA COLLECTION (Move Input to HTTP)

| File | Issue | What A3 Must Do |
|------|-------|-----------------|
| `src/phase_1_to_4/00_schema_definition.py` | • Blocks on `sys.stdin.read()` (waits for Ctrl+D)<br/>• Prints interactive prompts to stdout<br/>• Assumes CLI terminal available | **DEPRECATE:** Remove from main pipeline<br/>**MOVE:** Schema definition to FastAPI POST `/api/schema` endpoint<br/>**ACCEPT:** JSON payload in request body, validate with `validate_structure()` logic, persist to `initial_schema.json`<br/>**ONE-TIME:** If `initial_schema.json` exists, skip this step (no re-prompt on fetch/query) |
| `src/phase_6/CRUD_json_reader.py` | • Blocks on `sys.stdin.read()` for query JSON<br/>• Interactive CLI prompts<br/>• Assumes terminal available | **DEPRECATE:** CLI reader not usable in web<br/>**MOVE:** Query collection to FastAPI POST `/api/query` endpoint<br/>**ACCEPT:** JSON payload in request body, validate with `validate_structure()`, persist to `query.json`<br/>**INTEGRATE:** Flow directly to `CRUD_runner.query_runner()` (no interactive prompt) |

### DATA LOADING & PIPELINE (Add Lazy Init)

| File | Issue | What A3 Must Do |
|------|-------|-----------------|
| `src/phase_5/sql_pipeline.py` | • Contains CLI dispatcher: `python -m src.phase_5.sql_pipeline init/run` (lines 70+)<br/>• `argparse` setup for CLI<br/>• Print statements for status<br/>• Function `run_sql_pipeline()` is reusable but wrapped in CLI | **EXTRACT:** Keep `run_sql_pipeline(engine)` and `archive_processed_data()` functions (they're pure)<br/>**DELETE:** `main()` function (CLI dispatcher, lines 70+)<br/>**DELETE:** argparse setup<br/>**MOVE:** CLI invocation to FastAPI endpoint (e.g., POST `/api/ingest` calls `run_sql_pipeline()`)<br/>**KEEP:** Archive logic; integrate into pipeline orchestration |
| `src/phase_5/mongo_engine.py` | • ❌ Module-level side effect: ~~`mongo_client = MongoClient(...)`~~ removed (actually just inside functions, check line 7-8)<br/>• Functions `processNode()`, `determineMongoStrategy()`, `runMongoEngine()` are pure but not lazy-initialized | **REVIEW:** Confirm no top-level MongoClient creation<br/>**INTEGRATE:** Add lazy initialization hook in FastAPI lifespan (create client on first use)<br/>**KEEP:** All business logic functions as-is; they'll be called from FastAPI endpoints |
| `src/phase_6/CRUD_operations.py` | • ❌ Module-level side effects (lines 49–64):<br/>  - `sql_engine = SQLEngine()` + `sql_engine.initialize()`<br/>  - `mongo_client = MongoClient(...)`<br/>  - `tx_coordinator = TransactionCoordinator(...)`<br/>• All wrapped in try/except but run at import time<br/>• Prints status messages | **MOVE:** Lazy initialization to FastAPI lifespan (on app startup, not at import)<br/>**STORE:** Global engines in app.state (FastAPI pattern)<br/>**KEEP:** `create_operation()`, `read_operation()`, `update_operation()`, `delete_operation()` (pure business logic)<br/>**ADAPT:** Remove print() statements → return structured results for API response |
| `src/phase_6/CRUD_runner.py` | • Calls `query_parser()`, `analyze_query_databases()`, then dispatches to CRUD_operations<br/>• `query_runner()` is orchestration logic (acceptable) but assumes query.json pre-exists<br/>• Prints results to stdout | **KEEP:** Core logic `query_parser()`, `analyze_query_databases()` as pure functions<br/>**MOVE:** `query_runner()` to FastAPI endpoint OR integrate into endpoint handler directly<br/>**ADAPT:** Remove print() → return structured JSON for response<br/>**INPUT:** Accept parsed_query as function parameter (FastAPI validates before calling) |

---

## SCRAPPED — Superseded by Dashboard Architecture

These files assume CLI/standalone execution. A3 dashboard replaces their functionality via FastAPI endpoints and containerized deployment.

| File | Purpose | Why Scrapped | Replacement Strategy |
|------|---------|-----------|----------------------|
| `starter.py` | Manage Docker containers: start PostgreSQL/MongoDB/API | CLI subprocess spawning; hardcoded Windows Docker path; assumes terminal | Docker Compose managed by deployment ops team (K8s, Docker Desktop, or docker-compose CLI externally). Dashboard assumes infrastructure ready. If needed, provide a deployment guide (src/DEPLOYMENT.md) with `docker-compose up -d` command. |

---

## Integration Checklist for Agents

**Before modifying READ-ONLY files:** Don't. They're import-safe for FastAPI.

**Before modifying MODIFIABLE files:**
1. Confirm it's not re-used elsewhere (search codebase for imports)
2. Extract business logic functions into separate pure modules if needed
3. Ensure FastAPI lifespan initializes all DB connections (sql_engine, mongo_client) — not at import time
4. Replace `print()` → structured return values (JSON serializable)
5. Remove `sys.argv` → accept parameters from FastAPI request/payload
6. Remove `sys.exit()` → raise HTTPException or return error response

**Before importing SCRAPPED files:** Think twice. They're CLI-only and won't work in web context. If you need their logic, extract the pure functions first.

---

## Key Rules

### 🚫 Never Do This
- Import `main.py` directly in FastAPI (it has subprocess spawning)
- Invoke `starter.py` from web handler (it changes containers at runtime)
- Call `CRUD_json_reader.get_pasted_json()` from async task (it blocks on stdin)
- Call `sys.exit()` anywhere in FastAPI handler (terminate entire app)

### ✓ Always Do This
- Import functions from `phase_1_to_4/*.py` and `sql_engine.py` directly
- Call business logic (e.g., `cleaner.clean_recursive()`, `analyzer.analyze_records()`) from FastAPI endpoint
- Initialize DB engines once in FastAPI lifespan (on startup), store in `app.state`
- Return structured dicts/Pydantic models from all handler functions (never `print()`)
- Use `request.json()` for input instead of `sys.stdin.read()`

### 🔍 Red Flags (If You See These, Modify the File)
- `sys.stdin.read()` or `input()` → blocking call, move to HTTP
- `sys.exit()` or `sys.argv` → CLI dispatch, move to FastAPI route
- `print()` as primary output (not logging) → return structured response instead
- Module-level `SQLAlchemy.create_engine()`, `MongoClient()` → move to FastAPI lifespan
- `subprocess.Popen()` → externalize to deployment, don't call from handler

---

## Example: Converting a MODIFIABLE File to A3

**BEFORE (CLI-based):**
```python
# src/phase_6/CRUD_runner.py
def query_runner():
    parsed = query_parser()  # reads from query.json
    db_analysis = analyze_query_databases(parsed)
    result = handler_fn(parsed, db_analysis)
    print(json.dumps(result, indent=2))  # prints to stdout
    with open(QUERY_OUTPUT_FILE, 'w') as f:
        json.dump(result, f)
```

**AFTER (FastAPI-ready):**
```python
# src/phase_6/CRUD_runner.py (modified)
def query_runner(parsed_query: dict, db_analysis: dict = None) -> dict:
    """Pure function: no print(), no file I/O, returns structured result."""
    if not db_analysis:
        db_analysis = analyze_query_databases(parsed_query)
    result = handler_fn(parsed_query, db_analysis)
    return result  # caller (FastAPI) handles logging/file persistence

# dashboard_api.py (new FastAPI endpoint)
from fastapi import FastAPI, HTTPException
from src.phase_6.CRUD_runner import query_runner

app = FastAPI()

@app.post("/api/query")
async def execute_query(request: QueryRequest):
    try:
        query_dict = request.dict()
        result = query_runner(query_dict)
        
        # Persist if needed
        with open(QUERY_OUTPUT_FILE, 'w') as f:
            json.dump(result, f)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
```

---

## Files NOT Listed

These files are NOT mentioned in A2_BASELINE.md but may exist:
- `ACID/*.py` — A3-specific ACID validation tests (safe to modify, new feature)
- `docs/*.md` — Documentation (safe to modify/extend)
- `data/*.json` — Runtime artifacts (safe to delete/regenerate)
- `requirements.txt` — Dependencies (modify only if adding FastAPI, Pydantic, etc.)
- `Dockerfile` — Container spec (modify for dashboard backend if needed)
- `docker-compose.yml` — Orchestration spec (no modification needed if dashboard uses this)

Track all modifications to MODIFIABLE files in Git; no modifications should touch READ-ONLY files during A3.
