from copy import deepcopy

from .config import TEACHER_EMAIL_DOMAIN
from .db import db_execute, insert_and_get_id, query_all, query_one
from .errors import ApiError

LESSON_TYPE_ALIASES = {
    "lecture": "lecture",
    "лекция": "lecture",
    "дәріс": "lecture",
    "practical": "practical",
    "practice": "practical",
    "practical lesson": "practical",
    "практика": "practical",
    "практический": "practical",
    "практикалық": "practical",
    "lab": "lab",
    "laboratory": "lab",
    "лаборатория": "lab",
    "зертхана": "lab",
    "seminar": "seminar",
    "семинар": "seminar",
}


def normalize_number_fields(payload, fields):
    normalized = deepcopy(payload)
    for field in fields:
        if field in normalized and normalized[field] not in ("", None):
            try:
                normalized[field] = int(normalized[field])
            except (TypeError, ValueError):
                pass
    return normalized


def normalize_lesson_type(value):
    if value in (None, ""):
        return "lecture"
    normalized = str(value).strip().lower().replace("_", " ")
    compact = normalized.replace(" ", "_")
    return LESSON_TYPE_ALIASES.get(compact, LESSON_TYPE_ALIASES.get(normalized, str(value).strip().lower()))


def validate_teacher_email(email):
    normalized_email = (email or "").strip().lower()
    if not normalized_email.endswith(TEACHER_EMAIL_DOMAIN):
        raise ApiError(
            400,
            "teacher_email_domain_required",
            "Для преподавателя нужен email, оканчивающийся на @kazatu.edu.kz",
        )


def normalize_language(value, default="ru"):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"ru", "kk"} else default


def normalize_teaching_languages(value):
    raw_values = value.split(",") if isinstance(value, str) else (value or [])
    result = []
    for raw in raw_values:
        normalized = normalize_language(raw, "")
        if normalized and normalized not in result:
            result.append(normalized)
    return result or ["ru", "kk"]


def list_collection(connection, collection, query, user=None):
    if collection == "users":
        return query_all(
            connection,
            """
            SELECT id, email, full_name AS displayName, role, token
            FROM users
            ORDER BY id
            """,
        )

    if collection == "courses":
        return query_all(
            connection,
            """
            SELECT
                id, name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme, requires_computers
            FROM courses
            ORDER BY id
            """,
        )

    if collection == "teachers":
        return query_all(
            connection,
            """
            SELECT id, name, email, phone, department, weekly_hours_limit, teaching_languages
            FROM teachers
            ORDER BY id
            """,
        )

    if collection == "students":
        return query_all(
            connection,
            """
            SELECT id, name, email, department, programme, group_id, group_name, subgroup, language
            FROM students
            ORDER BY id
            """,
        )

    if collection == "rooms":
        return query_all(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment, department, available, computer_count
            FROM rooms
            ORDER BY id
            """,
        )

    if collection == "groups":
        return query_all(
            connection,
            """
            SELECT id, name, student_count, has_subgroups, language, study_course
            FROM groups
            ORDER BY id
            """,
        )

    if collection == "sections":
        return query_all(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type
            FROM sections
            ORDER BY id
            """,
        )

    clauses = []
    params = []
    semester = query.get("semester", [None])[0]
    year = query.get("year", [None])[0]
    from_sql = "FROM schedules s"
    if semester is not None:
        clauses.append("s.semester = ?")
        params.append(semester)
    if year is not None:
        clauses.append("s.year = ?")
        params.append(year)
    if collection == "schedules" and user and user.get("role") == "student":
        if not user.get("group_id"):
            return []
        clauses.append("s.group_id = ?")
        params.append(user["group_id"])
        if user.get("subgroup") in {"A", "B"}:
            clauses.append("(coalesce(s.subgroup, '') = '' OR upper(s.subgroup) = ?)")
            params.append(user["subgroup"])
        else:
            clauses.append("coalesce(s.subgroup, '') = ''")
    elif collection == "schedules" and user and user.get("role") == "teacher":
        from_sql += " LEFT JOIN teachers t ON t.id = s.teacher_id"
        clauses.append(
            "(lower(coalesce(t.email, '')) = lower(?) OR lower(coalesce(s.teacher_name, '')) = lower(?))"
        )
        params.append(user.get("email", ""))
        params.append(user.get("full_name", ""))

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return query_all(
        connection,
        f"""
        SELECT
            s.id, s.section_id, s.course_id, s.course_name, s.teacher_id, s.teacher_name, s.room_id, s.room_number,
            s.group_id, s.group_name, s.subgroup, s.day, s.start_hour, s.semester, s.year, s.algorithm
        {from_sql}
        {where_sql}
        ORDER BY s.day, s.start_hour, s.id
        """,
        tuple(params),
    )


def create_collection_item(connection, collection, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO courses (
                name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme, requires_computers
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                course_name,
                course_code,
                None,
                None,
                normalized.get("description", ""),
                normalized.get("year", normalized.get("study_year")),
                normalized.get("semester"),
                normalized.get("department", ""),
                normalized.get("instructor_id"),
                normalized.get("instructor_name", ""),
                normalized.get("programme", normalized.get("programme_name", "")),
                1 if normalized.get("requires_computers", 0) else 0,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme, requires_computers
            FROM courses
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["weekly_hours_limit", "max_hours_per_week"])
        validate_teacher_email(normalized.get("email"))
        teaching_languages = ",".join(normalize_teaching_languages(normalized.get("teaching_languages")))
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (name, email, phone, department, weekly_hours_limit, teaching_languages)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("department", normalized.get("specialization", "")),
                normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, department, weekly_hours_limit, teaching_languages
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO rooms (number, capacity, building, type, equipment, department, available, computer_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                normalized.get("department", ""),
                1 if normalized.get("available", normalized.get("is_available", 1)) else 0,
                normalized.get("computer_count", 0),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment, department, available, computer_count
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "groups":
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO groups (name, student_count, has_subgroups, language, study_course)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("student_count"),
                1 if normalized.get("has_subgroups", 0) else 0,
                group_language,
                normalized.get("study_course"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, student_count, has_subgroups, language, study_course
            FROM groups
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "sections":
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count"])
        normalized["lesson_type"] = normalize_lesson_type(normalized.get("lesson_type"))
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO sections (course_id, course_name, group_id, group_name, classes_count, lesson_type)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("group_id"),
                normalized.get("group_name", ""),
                normalized.get("classes_count", normalized.get("class_count")),
                normalized["lesson_type"],
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type
            FROM sections
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["section_id", "course_id", "teacher_id", "room_id", "group_id", "start_hour", "semester", "year"],
        )
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO schedules (
                section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("section_id"),
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("group_id"),
                normalized.get("group_name"),
                normalized.get("subgroup", ""),
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
                id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def update_collection_item(connection, collection, item_id, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        db_execute(
            connection,
            """
            UPDATE courses
            SET
                name = ?, code = ?, credits = ?, hours = ?, description = ?,
                year = ?, semester = ?, department = ?, instructor_id = ?, instructor_name = ?,
                programme = ?, requires_computers = ?
            WHERE id = ?
            """,
            (
                course_name,
                course_code,
                None,
                None,
                normalized.get("description", ""),
                normalized.get("year", normalized.get("study_year")),
                normalized.get("semester"),
                normalized.get("department", ""),
                normalized.get("instructor_id"),
                normalized.get("instructor_name", ""),
                normalized.get("programme", normalized.get("programme_name", "")),
                1 if normalized.get("requires_computers", 0) else 0,
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme, requires_computers
            FROM courses
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["weekly_hours_limit", "max_hours_per_week"])
        validate_teacher_email(normalized.get("email"))
        teaching_languages = ",".join(normalize_teaching_languages(normalized.get("teaching_languages")))
        db_execute(
            connection,
            """
            UPDATE teachers
            SET name = ?, email = ?, phone = ?, department = ?, weekly_hours_limit = ?, teaching_languages = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("department", normalized.get("specialization", "")),
                normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages,
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, department, weekly_hours_limit, teaching_languages
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        db_execute(
            connection,
            """
            UPDATE rooms
            SET number = ?, capacity = ?, building = ?, type = ?, equipment = ?, department = ?, available = ?, computer_count = ?
            WHERE id = ?
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                normalized.get("department", ""),
                1 if normalized.get("available", normalized.get("is_available", 1)) else 0,
                normalized.get("computer_count", 0),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment, department, available, computer_count
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "groups":
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        db_execute(
            connection,
            """
            UPDATE groups
            SET name = ?, student_count = ?, has_subgroups = ?, language = ?, study_course = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("student_count"),
                1 if normalized.get("has_subgroups", 0) else 0,
                group_language,
                normalized.get("study_course"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, student_count, has_subgroups, language, study_course
            FROM groups
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "sections":
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count"])
        normalized["lesson_type"] = normalize_lesson_type(normalized.get("lesson_type"))
        db_execute(
            connection,
            """
            UPDATE sections
            SET course_id = ?, course_name = ?, group_id = ?, group_name = ?, classes_count = ?, lesson_type = ?
            WHERE id = ?
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("group_id"),
                normalized.get("group_name", ""),
                normalized.get("classes_count", normalized.get("class_count")),
                normalized["lesson_type"],
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type
            FROM sections
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["section_id", "course_id", "teacher_id", "room_id", "group_id", "start_hour", "semester", "year"],
        )
        db_execute(
            connection,
            """
            UPDATE schedules
            SET
                section_id = ?, course_id = ?, course_name = ?, teacher_id = ?, teacher_name = ?,
                room_id = ?, room_number = ?, group_id = ?, group_name = ?, subgroup = ?,
                day = ?, start_hour = ?, semester = ?, year = ?, algorithm = ?
            WHERE id = ?
            """,
            (
                normalized.get("section_id"),
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("group_id"),
                normalized.get("group_name"),
                normalized.get("subgroup", ""),
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
                id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def delete_collection_item(connection, collection, item_id):
    if collection == "courses":
        db_execute(connection, "DELETE FROM schedules WHERE course_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM sections WHERE course_id = ?", (item_id,))
    elif collection == "groups":
        db_execute(connection, "DELETE FROM schedules WHERE group_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM sections WHERE group_id = ?", (item_id,))
        db_execute(connection, "UPDATE students SET group_id = NULL, group_name = '', subgroup = '' WHERE group_id = ?", (item_id,))
    elif collection == "teachers":
        db_execute(connection, "DELETE FROM schedules WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "UPDATE courses SET instructor_id = NULL, instructor_name = '' WHERE instructor_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'teacher' AND recipient_id = ?", (item_id,))
    elif collection == "rooms":
        db_execute(connection, "DELETE FROM schedules WHERE room_id = ?", (item_id,))
    elif collection == "students":
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'student' AND recipient_id = ?", (item_id,))
    db_execute(connection, f"DELETE FROM {collection} WHERE id = ?", (item_id,))
    connection.commit()
