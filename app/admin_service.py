from .auth_service import require_auth_user
from .config import DB_LOCK
from .db import db_execute, get_connection
from .errors import ApiError

CLEARABLE_COLLECTIONS = {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}


def _require_admin(headers):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return user


def clear_collection_data(headers, collection):
    _require_admin(headers)

    if collection not in CLEARABLE_COLLECTIONS:
        raise ApiError(400, "bad_request", "Неподдерживаемая коллекция")

    with DB_LOCK:
        with get_connection() as connection:
            if collection == "courses":
                db_execute(connection, "DELETE FROM schedules")
                db_execute(connection, "DELETE FROM sections")
                db_execute(connection, "DELETE FROM notifications")
            elif collection == "groups":
                db_execute(connection, "DELETE FROM schedules")
                db_execute(connection, "DELETE FROM sections")
                db_execute(connection, "DELETE FROM notifications")
                db_execute(
                    connection,
                    "UPDATE students SET group_id = NULL, group_name = '', subgroup = ''",
                )
            elif collection in {"teachers", "rooms"}:
                db_execute(connection, "DELETE FROM schedules")
                if collection == "teachers":
                    db_execute(connection, "DELETE FROM notifications")
            elif collection in {"students", "schedules"}:
                db_execute(connection, "DELETE FROM notifications")
            db_execute(connection, f"DELETE FROM {collection}")
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
        "collections": ["courses", "teachers", "students", "rooms", "groups", "schedules", "sections"],
    }
