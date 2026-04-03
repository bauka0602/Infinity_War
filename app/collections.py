from copy import deepcopy

from .db import db_execute, insert_and_get_id, query_all, query_one
from .errors import ApiError


def normalize_number_fields(payload, fields):
    normalized = deepcopy(payload)
    for field in fields:
        if field in normalized and normalized[field] not in ("", None):
            try:
                normalized[field] = int(normalized[field])
            except (TypeError, ValueError):
                pass
    return normalized


def list_collection(connection, collection, query):
    if collection == "users":
        return query_all(
            connection,
            """
            SELECT id, email, display_name AS displayName, role, token
            FROM users
            ORDER BY id
            """,
        )

    if collection == "courses":
        return query_all(
            connection,
            """
            SELECT id, name, code, credits, hours, description
            FROM courses
            ORDER BY id
            """,
        )

    if collection == "teachers":
        return query_all(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            ORDER BY id
            """,
        )

    if collection == "rooms":
        return query_all(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            ORDER BY id
            """,
        )

    clauses = []
    params = []
    semester = query.get("semester", [None])[0]
    year = query.get("year", [None])[0]
    if semester is not None:
        clauses.append("semester = ?")
        params.append(semester)
    if year is not None:
        clauses.append("year = ?")
        params.append(year)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return query_all(
        connection,
        f"""
        SELECT
            id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            day, start_hour, semester, year, algorithm
        FROM schedules
        {where_sql}
        ORDER BY day, start_hour, id
        """,
        tuple(params),
    )


def create_collection_item(connection, collection, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO courses (name, code, credits, hours, description)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("code"),
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            "SELECT id, name, code, credits, hours, description FROM courses WHERE id = ?",
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["max_hours_per_week"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (name, email, phone, specialization, max_hours_per_week)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("specialization", ""),
                normalized.get("max_hours_per_week"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO rooms (number, capacity, building, type, equipment)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["course_id", "teacher_id", "room_id", "start_hour", "semester", "year"],
        )
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO schedules (
                course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("algorithm"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def update_collection_item(connection, collection, item_id, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours"])
        db_execute(
            connection,
            """
            UPDATE courses
            SET name = ?, code = ?, credits = ?, hours = ?, description = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("code"),
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            "SELECT id, name, code, credits, hours, description FROM courses WHERE id = ?",
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["max_hours_per_week"])
        db_execute(
            connection,
            """
            UPDATE teachers
            SET name = ?, email = ?, phone = ?, specialization = ?, max_hours_per_week = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("specialization", ""),
                normalized.get("max_hours_per_week"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity"])
        db_execute(
            connection,
            """
            UPDATE rooms
            SET number = ?, capacity = ?, building = ?, type = ?, equipment = ?
            WHERE id = ?
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["course_id", "teacher_id", "room_id", "start_hour", "semester", "year"],
        )
        db_execute(
            connection,
            """
            UPDATE schedules
            SET
                course_id = ?, course_name = ?, teacher_id = ?, teacher_name = ?,
                room_id = ?, room_number = ?, day = ?, start_hour = ?,
                semester = ?, year = ?, algorithm = ?
            WHERE id = ?
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("algorithm"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def delete_collection_item(connection, collection, item_id):
    db_execute(connection, f"DELETE FROM {collection} WHERE id = ?", (item_id,))
    connection.commit()
