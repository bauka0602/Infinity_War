from ..auth.service import require_auth_user
from ..core.config import DB_LOCK
from ..core.db import db_execute, get_connection
from ..core.errors import ApiError
from ..rooms.availability import recompute_room_availability

CLEARABLE_COLLECTIONS = {"courses", "teachers", "students", "rooms", "groups", "sections"}


def _require_admin(headers):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return user


def clear_collection_data(headers, collection):
    _require_admin(headers)

    if collection == "schedules":
        raise ApiError(
            400,
            "bad_request",
            "Для расписания используйте сброс по выбранному семестру и году.",
        )

    if collection not in CLEARABLE_COLLECTIONS:
        raise ApiError(400, "bad_request", "Неподдерживаемая коллекция")

    with DB_LOCK:
        with get_connection() as connection:
            if collection == "courses":
                db_execute(connection, "DELETE FROM schedules")
                db_execute(connection, "DELETE FROM sections")
                db_execute(connection, "DELETE FROM course_components")
                db_execute(connection, "DELETE FROM iup_entries")
            elif collection == "groups":
                db_execute(connection, "DELETE FROM schedules")
                db_execute(connection, "DELETE FROM sections")
                db_execute(connection, "DELETE FROM iup_entries")
                db_execute(
                    connection,
                    "UPDATE students SET group_id = NULL, group_name = '', subgroup = ''",
                )
            elif collection in {"teachers", "rooms"}:
                db_execute(connection, "DELETE FROM schedules")
                if collection == "teachers":
                    db_execute(connection, "UPDATE courses SET instructor_id = NULL, instructor_name = ''")
                    db_execute(connection, "UPDATE course_components SET teacher_id = NULL, teacher_name = ''")
                    db_execute(connection, "UPDATE sections SET teacher_id = NULL, teacher_name = ''")
                    db_execute(connection, "DELETE FROM teacher_preference_requests")
                    db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'teacher'")
                else:
                    db_execute(connection, "DELETE FROM room_blocks")
            elif collection == "students":
                db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'student'")
            db_execute(connection, f"DELETE FROM {collection}")
            if collection in {"courses", "teachers", "groups", "schedules", "sections"}:
                recompute_room_availability(connection)
            connection.commit()

    return {"success": True, "collection": collection}


def clear_all_data(headers):
    _require_admin(headers)

    with DB_LOCK:
        with get_connection() as connection:
            for collection in (
                "notifications",
                "schedules",
                "sections",
                "teacher_preference_requests",
                "room_blocks",
                "iup_entries",
                "course_components",
                "courses",
                "teachers",
                "students",
                "rooms",
                "groups",
            ):
                db_execute(connection, f"DELETE FROM {collection}")
            connection.commit()

    return {
        "success": True,
        "collections": [
            "courses",
            "course_components",
            "iup_entries",
            "teacher_preference_requests",
            "teachers",
            "students",
            "rooms",
            "groups",
            "schedules",
            "sections",
        ],
    }


def clear_schedule_data(headers, semester=None, year=None):
    _require_admin(headers)

    with DB_LOCK:
        with get_connection() as connection:
            if semester is None and year is None:
                db_execute(connection, "DELETE FROM schedules")
            else:
                clauses = []
                params = []
                if semester is not None:
                    clauses.append("semester = ?")
                    params.append(semester)
                if year is not None:
                    clauses.append("year = ?")
                    params.append(year)
                where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                db_execute(connection, f"DELETE FROM schedules {where_sql}", tuple(params))
            recompute_room_availability(connection)
            connection.commit()

    return {
        "success": True,
        "collection": "schedules",
        "semester": semester,
        "year": year,
    }
