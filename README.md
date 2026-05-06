# Backend

FastAPI backend for timetable data, imports, section generation, and schedule generation.

## Stack

- FastAPI + Uvicorn
- SQLAlchemy ORM
- Alembic migrations
- SQLite locally, PostgreSQL/Neon via `DATABASE_URL`
- OR-Tools for CP-SAT optimization
- Optional Redis/RQ worker mode for background generation

## Setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Simple local run:

```bash
cd backend
../.venv/bin/python server.py
```

Direct Uvicorn run:

```bash
cd backend
../.venv/bin/uvicorn app.api.app:app --reload --port 8000
```

Default local DB is `backend/data/timetable.db`. Set `DATABASE_URL` for PostgreSQL.

## Migrations

Startup runs Alembic automatically by default.

```bash
cd backend
alembic upgrade head
```

Disable auto-upgrade with:

```bash
ALEMBIC_AUTO_UPGRADE=false
```

## Tests

```bash
cd backend
../.venv/bin/python -m pytest
```

## Main Structure

- `app/api/routers` - HTTP routes
- `app/api/schemas.py` - request/response schemas
- `app/models` - SQLAlchemy models
- `app/core` - config, ORM, migrations, logging, monitoring
- `app/imports` - ROP/IUP import parsing and storage
- `app/sections` - section generation and validation
- `app/schedule` - greedy, CP-SAT, jobs, payload building, workers
- `app/collections` - CRUD services and collection normalization
- `alembic` - database migrations

## Schedule Generation

Supported algorithms:

- `greedy` - primary fast production mode
- `cpsat_fast` - CP-SAT fast production mode, optimized for fast feasible schedules
- `cpsat` - full CP-SAT mode
- `hybrid` - CP-SAT with greedy fallback

Current practical note: `cpsat` is the default algorithm. It uses bounded room candidates, greedy warm-start data, quality soft constraints, and a relative optimality gap limit so it can spend time improving the timetable without running indefinitely. `cpsat_fast` stops at the first feasible CP-SAT schedule. `greedy` remains available as the fastest fallback/debug mode.

## Real Data Fixtures

Real import/test files are kept outside app code:

- `../fixtures/real-data/Disciplines` - ROP files
- `../fixtures/real-data/ИУП_Бизнес-информатика` - IUP PDF files
- `../fixtures/real-data/ИУП_Компьютерная-инженерия` - IUP PDF files
- `../fixtures/real-data/Queries` - ready CSV exports for smoke checks
