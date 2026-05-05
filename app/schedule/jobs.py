from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from threading import Lock, Thread
from uuid import uuid4

from sqlalchemy import delete, func, select, update

from ..core.config import DB_LOCK
from ..core.errors import ApiError
from ..core.monitoring import capture_exception
from ..core.orm import SessionLocal
from ..core.structured_logging import log_event
from ..models import Course, Group, Room, Schedule, ScheduleGenerationJob, Section, Teacher
from ..notifications.service import create_schedule_regeneration_notifications
from .service import build_schedule

try:
    from redis import Redis
    from rq import Queue
except ImportError:  # pragma: no cover - optional queue dependency
    Redis = None
    Queue = None

LOGGER = logging.getLogger(__name__)
_JOB_TTL = timedelta(hours=1)
_WORKER_POLL_INTERVAL_SECONDS = float(
    os.getenv("SCHEDULE_WORKER_POLL_INTERVAL_SECONDS", "5")
)
_RUNNING_JOB_STALE_SECONDS = float(
    os.getenv("SCHEDULE_RUNNING_JOB_STALE_SECONDS", str(20 * 60))
)
_EXECUTION_MODE = os.getenv("SCHEDULE_GENERATION_EXECUTION_MODE", "inline").lower()
_REDIS_URL = os.getenv("REDIS_URL", "").strip()
_RQ_QUEUE_NAME = os.getenv("SCHEDULE_RQ_QUEUE", "schedule-generation")
_jobs_lock = Lock()


def _utc_now():
    return datetime.now(timezone.utc)


def _utc_now_iso():
    return _utc_now().isoformat()


def _json_dumps(value):
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value):
    if value in (None, ""):
        return None
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def _cleanup_expired_jobs(connection):
    threshold = _utc_now() - _JOB_TTL
    connection.execute(
        delete(ScheduleGenerationJob).where(
            ScheduleGenerationJob.updated_at < threshold.isoformat()
        )
    )


def _mark_stale_running_jobs_failed(connection):
    threshold = _utc_now() - timedelta(seconds=_RUNNING_JOB_STALE_SECONDS)
    now = _utc_now_iso()
    connection.execute(
        update(ScheduleGenerationJob)
        .where(
            ScheduleGenerationJob.status == "running",
            ScheduleGenerationJob.updated_at < threshold.isoformat(),
        )
        .values(
            status="failed",
            error="Генерация расписания была остановлена. Запустите генерацию ещё раз.",
            error_code="schedule_generation_interrupted",
            details=None,
            updated_at=now,
            finished_at=now,
        )
    )


def _snapshot_job(row):
    def value(key):
        if isinstance(row, dict):
            return row.get(key)
        return getattr(row, key)

    details = _json_loads(value("details"))
    return {
        "jobId": value("job_id"),
        "status": value("status"),
        "semester": value("semester"),
        "year": value("year"),
        "algorithm": value("algorithm"),
        "createdAt": value("created_at"),
        "updatedAt": value("updated_at"),
        "result": _json_loads(value("result")),
        "error": value("error"),
        "errorCode": value("error_code"),
        "details": details,
        "progress": details.get("progress") if isinstance(details, dict) else None,
    }


def _log_schedule_job_event(event, job_id, **fields):
    log_event(LOGGER, event, job_id=job_id, **fields)


def _count_generation_inputs(semester, year):
    with SessionLocal() as session:
        sections = session.scalar(
            select(func.count())
            .select_from(Section)
            .join(Course, Course.id == Section.course_id)
            .where(
                Course.semester.in_((semester, semester + 2, semester + 4, semester + 6)),
                Section.lesson_type.in_(("lecture", "practical", "lab")),
            )
        )
        teachers = session.scalar(select(func.count()).select_from(Teacher))
        rooms = session.scalar(
            select(func.count())
            .select_from(Room)
            .where(func.coalesce(Room.available, 1) == 1)
        )
        groups = session.scalar(select(func.count()).select_from(Group))
    return {
        "sections": int(sections or 0),
        "teachers": int(teachers or 0),
        "rooms": int(rooms or 0),
        "groups": int(groups or 0),
    }


def _schedule_rows_for_notifications(semester, year):
    with SessionLocal() as session:
        rows = session.scalars(
            select(Schedule)
            .where(Schedule.semester == semester, Schedule.year == year)
            .order_by(Schedule.id)
        ).all()
        return [
            {
                "id": row.id,
                "section_id": row.section_id,
                "course_id": row.course_id,
                "course_name": row.course_name,
                "teacher_id": row.teacher_id,
                "teacher_name": row.teacher_name,
                "room_id": row.room_id,
                "room_number": row.room_number,
                "group_id": row.group_id,
                "group_name": row.group_name,
                "subgroup": row.subgroup,
                "day": row.day,
                "start_hour": row.start_hour,
                "semester": row.semester,
                "year": row.year,
                "algorithm": row.algorithm,
                "room_programme": row.room_programme,
                "room_programme_mismatch": row.room_programme_mismatch,
            }
            for row in rows
        ]


def _set_job_progress(job_id, progress):
    _set_job_state(job_id, details={"progress": progress})
    _log_schedule_job_event("schedule_generation_progress", job_id, **progress)


def _set_job_state(job_id, **updates):
    allowed_columns = {
        "status": "status",
        "result": "result",
        "error": "error",
        "errorCode": "error_code",
        "details": "details",
        "workerId": "worker_id",
        "startedAt": "started_at",
        "finishedAt": "finished_at",
    }
    values = {}
    for key, value in updates.items():
        column = allowed_columns.get(key)
        if column is None:
            continue
        if column in {"result", "details"}:
            value = _json_dumps(value)
        values[column] = value

    if not values:
        return

    values["updated_at"] = _utc_now_iso()

    with _jobs_lock:
        with SessionLocal() as session:
            session.execute(
                update(ScheduleGenerationJob)
                .where(ScheduleGenerationJob.job_id == job_id)
                .values(**values)
            )
            session.commit()


def _run_schedule_generation_job(
    job_id,
    semester,
    year,
    algorithm,
    *,
    worker_id=None,
    mark_running=True,
):
    started_monotonic = time.monotonic()

    def progress_callback(progress):
        elapsed = round(time.monotonic() - started_monotonic, 3)
        _set_job_progress(job_id, {**progress, "elapsedSeconds": elapsed})

    if mark_running:
        _set_job_state(
            job_id,
            status="running",
            workerId=worker_id,
            startedAt=_utc_now_iso(),
            finishedAt=None,
        )
    _log_schedule_job_event(
        "schedule_generation_started",
        job_id,
        semester=semester,
        year=year,
        algorithm=algorithm,
        worker_id=worker_id,
    )
    try:
        with DB_LOCK:
            input_counts = _count_generation_inputs(semester, year)
            _log_schedule_job_event(
                "schedule_generation_inputs_loaded",
                job_id,
                semester=semester,
                year=year,
                algorithm=algorithm,
                worker_id=worker_id,
                **input_counts,
            )
            previous_schedule = _schedule_rows_for_notifications(semester, year)
            generated = build_schedule(
                None,
                semester,
                year,
                algorithm,
                progress_callback=progress_callback,
            )
            updated_schedule = _schedule_rows_for_notifications(semester, year)
            create_schedule_regeneration_notifications(
                None,
                semester,
                year,
                previous_schedule,
                updated_schedule,
            )
        _set_job_state(
            job_id,
            status="completed",
            result={"scheduleCount": len(generated)},
            error=None,
            errorCode=None,
            details=None,
            finishedAt=_utc_now_iso(),
        )
        _log_schedule_job_event(
            "schedule_generation_completed",
            job_id,
            semester=semester,
            year=year,
            algorithm=algorithm,
            worker_id=worker_id,
            duration_seconds=round(time.monotonic() - started_monotonic, 3),
            schedule_count=len(generated),
        )
    except ApiError as exc:
        _set_job_state(
            job_id,
            status="failed",
            error=exc.message,
            errorCode=exc.code,
            details=exc.details or None,
            finishedAt=_utc_now_iso(),
        )
        _log_schedule_job_event(
            "schedule_generation_failed",
            job_id,
            semester=semester,
            year=year,
            algorithm=algorithm,
            worker_id=worker_id,
            duration_seconds=round(time.monotonic() - started_monotonic, 3),
            error_code=exc.code,
            error=exc.message,
        )
    except Exception as exc:
        capture_exception(
            exc,
            job_id=job_id,
            semester=semester,
            year=year,
            algorithm=algorithm,
        )
        _set_job_state(
            job_id,
            status="failed",
            error="Внутренняя ошибка сервера",
            errorCode="internal_server_error",
            details=None,
            finishedAt=_utc_now_iso(),
        )
        _log_schedule_job_event(
            "schedule_generation_failed",
            job_id,
            semester=semester,
            year=year,
            algorithm=algorithm,
            worker_id=worker_id,
            duration_seconds=round(time.monotonic() - started_monotonic, 3),
            error_code="internal_server_error",
            error=str(exc),
        )


def run_schedule_generation_rq_job(job_id, semester, year, algorithm, worker_id=None):
    _run_schedule_generation_job(
        job_id,
        semester,
        year,
        algorithm,
        worker_id=worker_id or os.getenv("SCHEDULE_WORKER_ID") or "rq-worker",
        mark_running=True,
    )


def _enqueue_rq_job(job_id, semester, year, algorithm):
    if Queue is None or Redis is None:
        raise RuntimeError("redis and rq are required for SCHEDULE_GENERATION_EXECUTION_MODE=rq")
    if not _REDIS_URL:
        raise RuntimeError("REDIS_URL is required for SCHEDULE_GENERATION_EXECUTION_MODE=rq")

    redis_connection = Redis.from_url(_REDIS_URL)
    queue = Queue(_RQ_QUEUE_NAME, connection=redis_connection)
    queue.enqueue(
        run_schedule_generation_rq_job,
        job_id,
        semester,
        year,
        algorithm,
        job_timeout=int(os.getenv("SCHEDULE_RQ_JOB_TIMEOUT_SECONDS", "1800")),
        result_ttl=int(os.getenv("SCHEDULE_RQ_RESULT_TTL_SECONDS", "3600")),
        failure_ttl=int(os.getenv("SCHEDULE_RQ_FAILURE_TTL_SECONDS", "86400")),
    )


def create_schedule_generation_job(semester, year, algorithm):
    job_id = uuid4().hex
    now = _utc_now_iso()
    job = ScheduleGenerationJob(
        job_id=job_id,
        status="queued",
        semester=semester,
        year=year,
        algorithm=algorithm,
        created_at=now,
        updated_at=now,
        result=None,
        error=None,
        error_code=None,
        details=None,
    )
    with _jobs_lock:
        with SessionLocal() as session:
            _cleanup_expired_jobs(session)
            session.add(job)
            session.commit()
    _log_schedule_job_event(
        "schedule_generation_queued",
        job_id,
        semester=semester,
        year=year,
        algorithm=algorithm,
        execution_mode=_EXECUTION_MODE,
    )

    if _EXECUTION_MODE == "rq":
        try:
            _enqueue_rq_job(job_id, semester, year, algorithm)
        except Exception as exc:
            _set_job_state(
                job_id,
                status="failed",
                error="Не удалось поставить генерацию в очередь Redis/RQ.",
                errorCode="queue_unavailable",
                details={"reason": str(exc)},
                finishedAt=_utc_now_iso(),
            )
            _log_schedule_job_event(
                "schedule_generation_queue_failed",
                job_id,
                semester=semester,
                year=year,
                algorithm=algorithm,
                error=str(exc),
            )
    elif _EXECUTION_MODE != "worker":
        worker = Thread(
            target=_run_schedule_generation_job,
            args=(job_id, semester, year, algorithm),
            daemon=True,
        )
        worker.start()
    return _snapshot_job(job)


def get_schedule_generation_job(job_id):
    with SessionLocal() as session:
        job = session.get(ScheduleGenerationJob, job_id)
        if job is None:
            raise ApiError(404, "record_not_found", "Задача генерации не найдена.")
        return _snapshot_job(job)


def claim_next_schedule_generation_job(worker_id=None):
    worker_id = worker_id or f"worker-{uuid4().hex[:12]}"
    with _jobs_lock:
        with SessionLocal() as session:
            _mark_stale_running_jobs_failed(session)
            session.commit()
            job = session.execute(
                select(ScheduleGenerationJob)
                .where(ScheduleGenerationJob.status == "queued")
                .order_by(ScheduleGenerationJob.created_at.asc())
                .limit(1)
            ).scalar_one_or_none()
            if job is None:
                return None

            now = _utc_now_iso()
            result = session.execute(
                update(ScheduleGenerationJob)
                .where(
                    ScheduleGenerationJob.job_id == job.job_id,
                    ScheduleGenerationJob.status == "queued",
                )
                .values(
                    status="running",
                    worker_id=worker_id,
                    started_at=now,
                    updated_at=now,
                    error=None,
                    error_code=None,
                    details=None,
                )
            )
            session.commit()

            if result.rowcount != 1:
                return None

            _log_schedule_job_event(
                "schedule_generation_claimed",
                job.job_id,
                semester=job.semester,
                year=job.year,
                algorithm=job.algorithm,
                worker_id=worker_id,
            )

            return {
                "jobId": job.job_id,
                "semester": job.semester,
                "year": job.year,
                "algorithm": job.algorithm,
                "workerId": worker_id,
            }


def run_schedule_generation_worker_once(worker_id=None):
    job = claim_next_schedule_generation_job(worker_id)
    if job is None:
        return False

    _run_schedule_generation_job(
        job["jobId"],
        job["semester"],
        job["year"],
        job["algorithm"],
        worker_id=job["workerId"],
        mark_running=False,
    )
    return True


def run_schedule_generation_worker_loop(worker_id=None, poll_interval=None):
    worker_id = worker_id or f"worker-{uuid4().hex[:12]}"
    poll_interval = (
        _WORKER_POLL_INTERVAL_SECONDS if poll_interval is None else poll_interval
    )

    while True:
        did_work = run_schedule_generation_worker_once(worker_id)
        if not did_work:
            time.sleep(poll_interval)
