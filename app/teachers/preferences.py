from datetime import datetime, timezone

from sqlalchemy import case, delete, func, select

from ..auth.service import require_auth_user
from ..core.config import DB_LOCK
from ..core.errors import ApiError
from ..core.orm import SessionLocal
from ..models import Teacher, TeacherPreferenceRequest
from ..schedule.time_slots import SCHEDULE_HOURS


VALID_DAYS = {"monday", "tuesday", "wednesday", "thursday", "friday"}
VALID_STATUSES = {"pending", "approved", "rejected"}
VALID_HOURS = set(SCHEDULE_HOURS)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _serialize_request(row):
    if row is None:
        return None
    return {
        "id": row["id"],
        "teacher_id": row["teacher_id"],
        "teacher_name": row["teacher_name"],
        "teacher_email": row.get("teacher_email", ""),
        "preferred_day": row["preferred_day"],
        "preferred_hour": row["preferred_hour"],
        "note": row.get("note", ""),
        "status": row["status"],
        "admin_comment": row.get("admin_comment", ""),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _validate_preference_payload(payload):
    preferred_day = str(payload.get("preferred_day") or "").strip().lower()
    preferred_hour = payload.get("preferred_hour")
    note = str(payload.get("note") or "").strip()

    try:
        preferred_hour = int(preferred_hour)
    except (TypeError, ValueError) as exc:
        raise ApiError(400, "bad_request", "Некорректный час предпочтения.") from exc

    if preferred_day not in VALID_DAYS:
        raise ApiError(400, "bad_request", "Некорректный день предпочтения.")
    if preferred_hour not in VALID_HOURS:
        raise ApiError(400, "bad_request", "Некорректный час предпочтения.")

    return {
        "preferred_day": preferred_day,
        "preferred_hour": preferred_hour,
        "note": note,
    }


def _ensure_teacher(user):
    if user["role"] != "teacher":
        raise ApiError(403, "forbidden", "Недостаточно прав.")


def _ensure_admin(user):
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав.")


def _teacher_preference_select():
    return select(
        TeacherPreferenceRequest.id.label("id"),
        TeacherPreferenceRequest.teacher_id.label("teacher_id"),
        TeacherPreferenceRequest.teacher_name.label("teacher_name"),
        Teacher.email.label("teacher_email"),
        TeacherPreferenceRequest.preferred_day.label("preferred_day"),
        TeacherPreferenceRequest.preferred_hour.label("preferred_hour"),
        TeacherPreferenceRequest.note.label("note"),
        TeacherPreferenceRequest.status.label("status"),
        TeacherPreferenceRequest.admin_comment.label("admin_comment"),
        TeacherPreferenceRequest.created_at.label("created_at"),
        TeacherPreferenceRequest.updated_at.label("updated_at"),
    ).join(Teacher, Teacher.id == TeacherPreferenceRequest.teacher_id)


def _find_conflict(session, teacher_id, preferred_day, preferred_hour, exclude_request_id=None):
    statement = select(TeacherPreferenceRequest).where(
        TeacherPreferenceRequest.preferred_day == preferred_day,
        TeacherPreferenceRequest.preferred_hour == preferred_hour,
        TeacherPreferenceRequest.status.in_(("pending", "approved")),
        TeacherPreferenceRequest.teacher_id != teacher_id,
    )
    if exclude_request_id is not None:
        statement = statement.where(TeacherPreferenceRequest.id != exclude_request_id)
    return session.scalar(statement)


def list_teacher_preference_requests(headers, mine=False):
    user = require_auth_user(headers)

    with DB_LOCK:
        with SessionLocal() as session:
            if mine:
                _ensure_teacher(user)
                statement = (
                    _teacher_preference_select()
                    .where(TeacherPreferenceRequest.teacher_id == user["id"])
                    .order_by(
                        TeacherPreferenceRequest.created_at.desc(),
                        TeacherPreferenceRequest.id.desc(),
                    )
                )
            else:
                _ensure_admin(user)
                statement = _teacher_preference_select().order_by(
                    case(
                        (TeacherPreferenceRequest.status == "pending", 0),
                        (TeacherPreferenceRequest.status == "approved", 1),
                        else_=2,
                    ),
                    TeacherPreferenceRequest.created_at.desc(),
                    TeacherPreferenceRequest.id.desc(),
                )
            rows = session.execute(statement).mappings().all()
    return [_serialize_request(row) for row in rows]


def create_teacher_preference_request(headers, payload):
    user = require_auth_user(headers)
    _ensure_teacher(user)
    normalized = _validate_preference_payload(payload)

    with DB_LOCK:
        with SessionLocal() as session:
            conflict = _find_conflict(
                session,
                user["id"],
                normalized["preferred_day"],
                normalized["preferred_hour"],
            )
            if conflict:
                raise ApiError(
                    400,
                    "bad_request",
                    f"Слот уже занят заявкой преподавателя {conflict.teacher_name}.",
                )

            duplicate = session.scalar(
                select(TeacherPreferenceRequest.id).where(
                    TeacherPreferenceRequest.teacher_id == user["id"],
                    TeacherPreferenceRequest.preferred_day == normalized["preferred_day"],
                    TeacherPreferenceRequest.preferred_hour == normalized["preferred_hour"],
                    TeacherPreferenceRequest.status.in_(("pending", "approved")),
                )
            )
            if duplicate:
                raise ApiError(400, "bad_request", "Вы уже отправили запрос на этот слот.")

            now = _now_iso()
            request_row = TeacherPreferenceRequest(
                teacher_id=user["id"],
                teacher_name=user["full_name"],
                preferred_day=normalized["preferred_day"],
                preferred_hour=normalized["preferred_hour"],
                note=normalized["note"],
                status="pending",
                admin_comment="",
                created_at=now,
                updated_at=now,
            )
            session.add(request_row)
            session.commit()

            created = session.execute(
                _teacher_preference_select().where(
                    TeacherPreferenceRequest.id == request_row.id
                )
            ).mappings().one()
    return _serialize_request(created)


def update_teacher_preference_status(headers, request_id, payload):
    user = require_auth_user(headers)
    _ensure_admin(user)

    status = str(payload.get("status") or "").strip().lower()
    admin_comment = str(payload.get("admin_comment") or "").strip()
    if status not in VALID_STATUSES - {"pending"}:
        raise ApiError(400, "bad_request", "Некорректный статус заявки.")

    with DB_LOCK:
        with SessionLocal() as session:
            existing = session.get(TeacherPreferenceRequest, request_id)
            if existing is None:
                raise ApiError(404, "record_not_found", "Запрос преподавателя не найден.")

            if status == "approved":
                conflict = _find_conflict(
                    session,
                    existing.teacher_id,
                    existing.preferred_day,
                    existing.preferred_hour,
                    exclude_request_id=request_id,
                )
                if conflict:
                    raise ApiError(
                        400,
                        "bad_request",
                        f"Слот уже занят заявкой преподавателя {conflict.teacher_name}.",
                    )

            existing.status = status
            existing.admin_comment = admin_comment
            existing.updated_at = _now_iso()
            session.commit()

            updated = session.execute(
                _teacher_preference_select().where(
                    TeacherPreferenceRequest.id == request_id
                )
            ).mappings().one()
    return _serialize_request(updated)


def delete_teacher_preference_request(headers, request_id):
    user = require_auth_user(headers)
    _ensure_admin(user)

    with DB_LOCK:
        with SessionLocal() as session:
            existing = session.execute(
                _teacher_preference_select().where(
                    TeacherPreferenceRequest.id == request_id
                )
            ).mappings().one_or_none()
            if existing is None:
                raise ApiError(404, "record_not_found", "Запрос преподавателя не найден.")

            session.execute(
                delete(TeacherPreferenceRequest).where(
                    TeacherPreferenceRequest.id == request_id
                )
            )
            session.commit()

    return {"deleted": True, "item": _serialize_request(existing)}


def delete_all_teacher_preference_requests(headers):
    user = require_auth_user(headers)
    _ensure_admin(user)

    with DB_LOCK:
        with SessionLocal() as session:
            total = int(
                session.scalar(select(func.count()).select_from(TeacherPreferenceRequest))
                or 0
            )
            session.execute(delete(TeacherPreferenceRequest))
            session.commit()

    return {"deleted": True, "count": total}


def get_approved_teacher_preferences(connection=None):
    with SessionLocal() as session:
        rows = session.execute(
            select(
                TeacherPreferenceRequest.teacher_id.label("teacher_id"),
                TeacherPreferenceRequest.preferred_day.label("preferred_day"),
                TeacherPreferenceRequest.preferred_hour.label("preferred_hour"),
            )
            .where(TeacherPreferenceRequest.status == "approved")
            .order_by(
                TeacherPreferenceRequest.teacher_id,
                TeacherPreferenceRequest.preferred_day,
                TeacherPreferenceRequest.preferred_hour,
            )
        ).mappings().all()
        return [dict(row) for row in rows]
