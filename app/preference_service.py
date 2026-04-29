from datetime import datetime, timezone

from .auth_service import require_auth_user
from .config import DB_LOCK
from .db import db_execute, get_connection, insert_and_get_id, query_all, query_one
from .errors import ApiError
from .time_slots import SCHEDULE_HOURS


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


def _find_conflict(connection, teacher_id, preferred_day, preferred_hour, exclude_request_id=None):
    query = """
        SELECT id, teacher_id, teacher_name, preferred_day, preferred_hour, status
        FROM teacher_preference_requests
        WHERE preferred_day = ? AND preferred_hour = ? AND status IN ('pending', 'approved') AND teacher_id <> ?
    """
    params = [preferred_day, preferred_hour, teacher_id]
    if exclude_request_id is not None:
        query += " AND id <> ?"
        params.append(exclude_request_id)

    return query_one(connection, query, tuple(params))


def list_teacher_preference_requests(headers, mine=False):
    user = require_auth_user(headers)

    with DB_LOCK:
        with get_connection() as connection:
            if mine:
                _ensure_teacher(user)
                rows = query_all(
                    connection,
                    """
                    SELECT
                        r.id,
                        r.teacher_id,
                        r.teacher_name,
                        t.email AS teacher_email,
                        r.preferred_day,
                        r.preferred_hour,
                        r.note,
                        r.status,
                        r.admin_comment,
                        r.created_at,
                        r.updated_at
                    FROM teacher_preference_requests r
                    JOIN teachers t ON t.id = r.teacher_id
                    WHERE r.teacher_id = ?
                    ORDER BY r.created_at DESC, r.id DESC
                    """,
                    (user["id"],),
                )
            else:
                _ensure_admin(user)
                rows = query_all(
                    connection,
                    """
                    SELECT
                        r.id,
                        r.teacher_id,
                        r.teacher_name,
                        t.email AS teacher_email,
                        r.preferred_day,
                        r.preferred_hour,
                        r.note,
                        r.status,
                        r.admin_comment,
                        r.created_at,
                        r.updated_at
                    FROM teacher_preference_requests r
                    JOIN teachers t ON t.id = r.teacher_id
                    ORDER BY
                        CASE r.status
                            WHEN 'pending' THEN 0
                            WHEN 'approved' THEN 1
                            ELSE 2
                        END,
                        r.created_at DESC,
                        r.id DESC
                    """,
                )
    return [_serialize_request(row) for row in rows]


def create_teacher_preference_request(headers, payload):
    user = require_auth_user(headers)
    _ensure_teacher(user)
    normalized = _validate_preference_payload(payload)

    with DB_LOCK:
        with get_connection() as connection:
            conflict = _find_conflict(
                connection,
                user["id"],
                normalized["preferred_day"],
                normalized["preferred_hour"],
            )
            if conflict:
                raise ApiError(
                    400,
                    "bad_request",
                    f"Слот уже занят заявкой преподавателя {conflict['teacher_name']}.",
                )

            duplicate = query_one(
                connection,
                """
                SELECT id
                FROM teacher_preference_requests
                WHERE teacher_id = ? AND preferred_day = ? AND preferred_hour = ? AND status IN ('pending', 'approved')
                """,
                (user["id"], normalized["preferred_day"], normalized["preferred_hour"]),
            )
            if duplicate:
                raise ApiError(400, "bad_request", "Вы уже отправили запрос на этот слот.")

            request_id = insert_and_get_id(
                connection,
                """
                INSERT INTO teacher_preference_requests (
                    teacher_id, teacher_name, preferred_day, preferred_hour, note, status, admin_comment, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', '', ?, ?)
                """,
                (
                    user["id"],
                    user["full_name"],
                    normalized["preferred_day"],
                    normalized["preferred_hour"],
                    normalized["note"],
                    _now_iso(),
                    _now_iso(),
                ),
            )
            connection.commit()

            created = query_one(
                connection,
                """
                SELECT
                    r.id,
                    r.teacher_id,
                    r.teacher_name,
                    t.email AS teacher_email,
                    r.preferred_day,
                    r.preferred_hour,
                    r.note,
                    r.status,
                    r.admin_comment,
                    r.created_at,
                    r.updated_at
                FROM teacher_preference_requests r
                JOIN teachers t ON t.id = r.teacher_id
                WHERE r.id = ?
                """,
                (request_id,),
            )
    return _serialize_request(created)


def update_teacher_preference_status(headers, request_id, payload):
    user = require_auth_user(headers)
    _ensure_admin(user)

    status = str(payload.get("status") or "").strip().lower()
    admin_comment = str(payload.get("admin_comment") or "").strip()
    if status not in VALID_STATUSES - {"pending"}:
        raise ApiError(400, "bad_request", "Некорректный статус заявки.")

    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(
                connection,
                """
                SELECT id, teacher_id, teacher_name, preferred_day, preferred_hour, note, status, admin_comment, created_at, updated_at
                FROM teacher_preference_requests
                WHERE id = ?
                """,
                (request_id,),
            )
            if existing is None:
                raise ApiError(404, "record_not_found", "Запрос преподавателя не найден.")

            if status == "approved":
                conflict = _find_conflict(
                    connection,
                    existing["teacher_id"],
                    existing["preferred_day"],
                    existing["preferred_hour"],
                    exclude_request_id=request_id,
                )
                if conflict:
                    raise ApiError(
                        400,
                        "bad_request",
                        f"Слот уже занят заявкой преподавателя {conflict['teacher_name']}.",
                    )

            db_execute(
                connection,
                """
                UPDATE teacher_preference_requests
                SET status = ?, admin_comment = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, admin_comment, _now_iso(), request_id),
            )
            connection.commit()

            updated = query_one(
                connection,
                """
                SELECT
                    r.id,
                    r.teacher_id,
                    r.teacher_name,
                    t.email AS teacher_email,
                    r.preferred_day,
                    r.preferred_hour,
                    r.note,
                    r.status,
                    r.admin_comment,
                    r.created_at,
                    r.updated_at
                FROM teacher_preference_requests r
                JOIN teachers t ON t.id = r.teacher_id
                WHERE r.id = ?
                """,
                (request_id,),
            )
    return _serialize_request(updated)


def delete_teacher_preference_request(headers, request_id):
    user = require_auth_user(headers)
    _ensure_admin(user)

    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(
                connection,
                """
                SELECT
                    r.id,
                    r.teacher_id,
                    r.teacher_name,
                    t.email AS teacher_email,
                    r.preferred_day,
                    r.preferred_hour,
                    r.note,
                    r.status,
                    r.admin_comment,
                    r.created_at,
                    r.updated_at
                FROM teacher_preference_requests r
                JOIN teachers t ON t.id = r.teacher_id
                WHERE r.id = ?
                """,
                (request_id,),
            )
            if existing is None:
                raise ApiError(404, "record_not_found", "Запрос преподавателя не найден.")

            db_execute(
                connection,
                "DELETE FROM teacher_preference_requests WHERE id = ?",
                (request_id,),
            )
            connection.commit()

    return {"deleted": True, "item": _serialize_request(existing)}


def delete_all_teacher_preference_requests(headers):
    user = require_auth_user(headers)
    _ensure_admin(user)

    with DB_LOCK:
        with get_connection() as connection:
            row = query_one(
                connection,
                "SELECT COUNT(*) AS total FROM teacher_preference_requests",
            )
            total = int(row["total"] or 0) if row else 0
            db_execute(connection, "DELETE FROM teacher_preference_requests")
            connection.commit()

    return {"deleted": True, "count": total}


def get_approved_teacher_preferences(connection):
    return query_all(
        connection,
        """
        SELECT teacher_id, preferred_day, preferred_hour
        FROM teacher_preference_requests
        WHERE status = 'approved'
        ORDER BY teacher_id, preferred_day, preferred_hour
        """,
    )
