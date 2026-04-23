from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Lock, Thread
from uuid import uuid4

from .config import DB_LOCK
from .db import get_connection, query_all
from .errors import ApiError
from .notification_service import create_schedule_regeneration_notifications
from .scheduling import build_schedule

_JOB_TTL = timedelta(hours=1)
_jobs = {}
_jobs_lock = Lock()


def _utc_now():
    return datetime.now(timezone.utc)


def _utc_now_iso():
    return _utc_now().isoformat()


def _cleanup_expired_jobs():
    threshold = _utc_now() - _JOB_TTL
    expired_ids = [
        job_id
        for job_id, job in _jobs.items()
        if datetime.fromisoformat(job["updatedAt"]) < threshold
    ]
    for job_id in expired_ids:
        _jobs.pop(job_id, None)


def _snapshot_job(job):
    return {
        "jobId": job["jobId"],
        "status": job["status"],
        "semester": job["semester"],
        "year": job["year"],
        "algorithm": job["algorithm"],
        "createdAt": job["createdAt"],
        "updatedAt": job["updatedAt"],
        "result": job.get("result"),
        "error": job.get("error"),
        "errorCode": job.get("errorCode"),
        "details": job.get("details"),
    }


def _set_job_state(job_id, **updates):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return
        job.update(updates)
        job["updatedAt"] = _utc_now_iso()


def _run_schedule_generation_job(job_id, semester, year, algorithm):
    _set_job_state(job_id, status="running")
    try:
        with DB_LOCK:
            with get_connection() as connection:
                previous_schedule = query_all(
                    connection,
                    """
                    SELECT
                        id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                        group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
                        room_programme, room_programme_mismatch
                    FROM schedules
                    WHERE semester = ? AND year = ?
                    ORDER BY id
                    """,
                    (semester, year),
                )
                generated = build_schedule(connection, semester, year, algorithm)
                updated_schedule = query_all(
                    connection,
                    """
                    SELECT
                        id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                        group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
                        room_programme, room_programme_mismatch
                    FROM schedules
                    WHERE semester = ? AND year = ?
                    ORDER BY id
                    """,
                    (semester, year),
                )
                create_schedule_regeneration_notifications(
                    connection,
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
        )
    except ApiError as exc:
        _set_job_state(
            job_id,
            status="failed",
            error=exc.message,
            errorCode=exc.code,
            details=exc.details or None,
        )
    except Exception:
        _set_job_state(
            job_id,
            status="failed",
            error="Внутренняя ошибка сервера",
            errorCode="internal_server_error",
            details=None,
        )


def create_schedule_generation_job(semester, year, algorithm):
    job_id = uuid4().hex
    job = {
        "jobId": job_id,
        "status": "queued",
        "semester": semester,
        "year": year,
        "algorithm": algorithm,
        "createdAt": _utc_now_iso(),
        "updatedAt": _utc_now_iso(),
        "result": None,
        "error": None,
        "errorCode": None,
        "details": None,
    }
    with _jobs_lock:
        _cleanup_expired_jobs()
        _jobs[job_id] = job

    worker = Thread(
        target=_run_schedule_generation_job,
        args=(job_id, semester, year, algorithm),
        daemon=True,
    )
    worker.start()
    return _snapshot_job(job)


def get_schedule_generation_job(job_id):
    with _jobs_lock:
        _cleanup_expired_jobs()
        job = _jobs.get(job_id)
        if job is None:
            raise ApiError(404, "record_not_found", "Задача генерации не найдена.")
        return _snapshot_job(job)
