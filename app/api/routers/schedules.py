from datetime import date

from fastapi import APIRouter, Request

from ...auth_service import require_auth_user
from ...collections import generate_sections_from_components
from ...config import DB_LOCK
from ...db import get_connection
from ...errors import ApiError
from ...job_store import create_schedule_generation_job, get_schedule_generation_job
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
    algorithm = payload.get("algorithm") or "greedy"
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

