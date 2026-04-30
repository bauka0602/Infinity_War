from datetime import date

from fastapi import APIRouter, Request

from ...auth.service import require_auth_user
from ...collections.service import generate_sections_from_components
from ...core.config import DB_LOCK
from ...core.db import get_connection
from ...core.errors import ApiError
from ...schedule.jobs import create_schedule_generation_job, get_schedule_generation_job
from ...schedule import DEFAULT_SCHEDULE_ALGORITHM, normalize_schedule_algorithm
from ...sections.generation import (
    build_validation_report,
    generate_sections_from_iup,
    preview_sections_from_iup,
)
from ..common import read_json_body

router = APIRouter()


@router.post("/schedules/generate", status_code=202)
async def schedule_generate(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    payload = await read_json_body(request)
    semester = int(payload.get("semester") or 1)
    year = int(payload.get("year") or date.today().year)
    algorithm = normalize_schedule_algorithm(payload.get("algorithm") or DEFAULT_SCHEDULE_ALGORITHM)
    return create_schedule_generation_job(semester, year, algorithm)


@router.get("/schedules/generate/{job_id}")
def schedule_generate_status(job_id: str, request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return get_schedule_generation_job(job_id)


@router.post("/sections/generate")
async def sections_generate(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    payload = await read_json_body(request)
    with DB_LOCK:
        with get_connection() as connection:
            return generate_sections_from_components(connection, payload)


@router.post("/sections/generate-from-iup")
async def sections_generate_from_iup(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    payload = await read_json_body(request)
    with DB_LOCK:
        with get_connection() as connection:
            return generate_sections_from_iup(connection, payload)


@router.post("/sections/generate-from-iup/preview")
async def sections_generate_from_iup_preview(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    payload = await read_json_body(request)
    with DB_LOCK:
        with get_connection() as connection:
            return preview_sections_from_iup(connection, payload)


@router.get("/validation/report")
def validation_report(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with DB_LOCK:
        with get_connection() as connection:
            return build_validation_report(connection)
