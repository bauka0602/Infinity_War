import asyncio
import json
from typing import Any
from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from ...auth.service import require_auth_user
from ...collections.service import generate_sections_from_components
from ...core.config import DB_LOCK
from ...core.errors import ApiError
from ...schedule.jobs import (
    cancel_schedule_generation_job,
    create_schedule_generation_job,
    get_schedule_generation_job,
)
from ...schedule import DEFAULT_SCHEDULE_ALGORITHM, normalize_schedule_algorithm
from ...sections.generation import (
    build_validation_report,
    generate_sections_from_iup,
    preview_sections_from_iup,
)
from ..schemas import (
    ScheduleGenerateRequest,
    ScheduleGenerationJobResponse,
    SectionGenerateRequest,
)

router = APIRouter()


@router.post(
    "/schedules/generate",
    status_code=202,
    response_model=ScheduleGenerationJobResponse,
)
async def schedule_generate(payload: ScheduleGenerateRequest, request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    semester = int(payload.semester or 1)
    year = int(payload.year or date.today().year)
    algorithm = normalize_schedule_algorithm(payload.algorithm or DEFAULT_SCHEDULE_ALGORITHM)
    return create_schedule_generation_job(semester, year, algorithm)


@router.get(
    "/schedules/generate/{job_id}",
    response_model=ScheduleGenerationJobResponse,
)
def schedule_generate_status(job_id: str, request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return get_schedule_generation_job(job_id)


@router.post(
    "/schedules/generate/{job_id}/cancel",
    response_model=ScheduleGenerationJobResponse,
)
def schedule_generate_cancel(job_id: str, request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return cancel_schedule_generation_job(job_id)


@router.get("/schedules/generate/{job_id}/events")
async def schedule_generate_events(job_id: str, request: Request):
    token = request.query_params.get("token", "")
    user = require_auth_user({"Authorization": f"Bearer {token}"})
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    async def event_stream():
        last_signature = None
        while True:
            if await request.is_disconnected():
                break

            job = get_schedule_generation_job(job_id)
            signature = (
                job.get("status"),
                job.get("updatedAt"),
                job.get("errorCode"),
                json.dumps(job.get("progress"), sort_keys=True, ensure_ascii=False),
            )
            if signature != last_signature:
                last_signature = signature
                yield (
                    "event: status\n"
                    f"data: {json.dumps(job, ensure_ascii=False)}\n\n"
                )
                if job.get("status") in {"completed", "failed"}:
                    break
            else:
                yield "event: heartbeat\ndata: {}\n\n"

            await asyncio.sleep(2)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/sections/generate")
async def sections_generate(payload: SectionGenerateRequest, request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with DB_LOCK:
        return generate_sections_from_components(None, payload.model_dump(exclude_none=True))


@router.post("/sections/generate-from-iup")
async def sections_generate_from_iup(payload: dict[str, Any], request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with DB_LOCK:
        return generate_sections_from_iup(None, payload)


@router.post("/sections/generate-from-iup/preview")
async def sections_generate_from_iup_preview(payload: dict[str, Any], request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with DB_LOCK:
        return preview_sections_from_iup(None, payload)


@router.get("/validation/report")
def validation_report(request: Request):
    user = require_auth_user(request.headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with DB_LOCK:
        return build_validation_report(None)
