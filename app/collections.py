import re
from copy import deepcopy
from datetime import date
from math import ceil

from .config import TEACHER_EMAIL_DOMAIN
from .db import db_execute, insert_and_get_id, query_all, query_one
from .errors import ApiError
from .lesson_rules import requires_computers_for_component

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

SPECIALTY_PROGRAMME_ALIASES = {
    "би": "Бизнес-информатика",
    "бизи": "Бизнес-информатика",
    "ки": "Компьютерная инженерия",
    "ки сопр": "Компьютерная инженерия (СОПР)",
    "сопр": "Компьютерная инженерия (СОПР)",
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


def positive_int(value, default=1):
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def normalize_lesson_type(value):
    if value in (None, ""):
        return "lecture"
    normalized = str(value).strip().lower().replace("_", " ")
    compact = normalized.replace(" ", "_")
    return LESSON_TYPE_ALIASES.get(compact, LESSON_TYPE_ALIASES.get(normalized, str(value).strip().lower()))


def normalize_subgroup_mode(value, lesson_type="lecture"):
    if lesson_type == "lecture":
        return "none"
    normalized = str(value or "auto").strip().lower()
    return normalized if normalized in {"none", "auto", "forced"} else "auto"


def section_requires_computers(lesson_type, course_code="", course_name="", study_year=None):
    return 1 if requires_computers_for_component(lesson_type, course_code, course_name, study_year) else 0


def resolve_section_teacher(connection, course_id, lesson_type, payload):
    teacher_id = payload.get("teacher_id")
    teacher_name = payload.get("teacher_name", "")

    if teacher_id:
        teacher = query_one(
            connection,
            "SELECT id, name FROM teachers WHERE id = ?",
            (teacher_id,),
        )
        if teacher:
            return teacher["id"], teacher["name"]

    component_teacher = query_one(
        connection,
        """
        SELECT teacher_id, teacher_name
        FROM course_components
        WHERE course_id = ?
          AND lesson_type = ?
          AND teacher_id IS NOT NULL
        ORDER BY academic_period, id
        LIMIT 1
        """,
        (course_id, lesson_type),
    )
    if component_teacher:
        return component_teacher["teacher_id"], component_teacher.get("teacher_name", "")

    course_teacher = query_one(
        connection,
        "SELECT instructor_id, instructor_name FROM courses WHERE id = ?",
        (course_id,),
    )
    if course_teacher:
        return course_teacher.get("instructor_id"), course_teacher.get("instructor_name", "")

    return None, teacher_name


def _same_programme(left, right):
    def normalize(value):
        normalized = str(value or "").strip().lower()
        normalized = re.sub(r"\s*\([^)]*\)\s*$", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized

    left_normalized = normalize(left)
    right_normalized = normalize(right)
    return bool(left_normalized and right_normalized and left_normalized == right_normalized)


def generate_sections_from_components(connection, payload):
    semester = payload.get("semester")
    study_course = payload.get("study_course") or payload.get("year")
    programme = str(payload.get("programme") or "").strip()

    semester = int(semester) if semester else None
    study_course = int(study_course) if study_course else None
    all_groups = query_all(
        connection,
        """
        SELECT id, name, programme, study_course
        FROM groups
        WHERE programme IS NOT NULL
          AND programme != ''
          AND study_course IS NOT NULL
        ORDER BY study_course, programme, name
        """,
    )
    groups = [
        group
        for group in all_groups
        if (not study_course or int(group.get("study_course") or 0) == study_course)
        and (not programme or _same_programme(group.get("programme"), programme))
    ]
    component_clauses = ["cc.lesson_type IN ('lecture', 'practical', 'lab')"]
    component_params = []
    if semester:
        component_clauses.append("cc.academic_period = ?")
        component_params.append(semester)
    if study_course:
        component_clauses.append("c.year = ?")
        component_params.append(study_course)
    where_sql = " AND ".join(component_clauses)
    components = [
        component
        for component in query_all(
            connection,
            f"""
            SELECT
                cc.course_id,
                cc.course_name,
                cc.lesson_type,
                cc.weekly_classes,
                cc.requires_computers,
                c.programme,
                c.year,
                c.semester
            FROM course_components cc
            JOIN courses c ON c.id = cc.course_id
            WHERE {where_sql}
            ORDER BY c.year, c.programme, c.name, cc.lesson_type
            """,
            tuple(component_params),
        )
        if (not programme or _same_programme(component.get("programme"), programme))
    ]

    if not groups or not components:
        return {
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "missing": {
                "groups": len(groups) == 0,
                "components": len(components) == 0,
            },
            "sections": [],
        }

    inserted = 0
    updated = 0
    generated_sections = []
    for component in components:
        matching_groups = [
            group
            for group in groups
            if _same_programme(group.get("programme"), component.get("programme"))
            and int(group.get("study_course") or 0) == int(component.get("year") or 0)
        ]
        for group in matching_groups:
            lesson_type = normalize_lesson_type(component["lesson_type"])
            classes_count = positive_int(component.get("weekly_classes"), 1)
            subgroup_mode = normalize_subgroup_mode("none" if lesson_type == "lecture" else "auto", lesson_type)
            subgroup_count = 1
            requires_computers = 1 if component.get("requires_computers") else 0
            teacher_id, teacher_name = resolve_section_teacher(
                connection,
                component["course_id"],
                lesson_type,
                {},
            )
            existing = query_one(
                connection,
                """
                SELECT id
                FROM sections
                WHERE course_id = ?
                  AND group_id = ?
                  AND lesson_type = ?
                LIMIT 1
                """,
                (component["course_id"], group["id"], lesson_type),
            )
            params = (
                component["course_id"],
                component["course_name"],
                group["id"],
                group["name"],
                classes_count,
                lesson_type,
                subgroup_mode,
                subgroup_count,
                requires_computers,
                teacher_id,
                teacher_name or "",
            )

            if existing:
                db_execute(
                    connection,
                    """
                    UPDATE sections
                    SET course_id = ?, course_name = ?, group_id = ?, group_name = ?,
                        classes_count = ?, lesson_type = ?, subgroup_mode = ?, subgroup_count = ?,
                        requires_computers = ?, teacher_id = ?, teacher_name = ?
                    WHERE id = ?
                    """,
                    (*params, existing["id"]),
                )
                section_id = existing["id"]
                updated += 1
            else:
                section_id = insert_and_get_id(
                    connection,
                    """
                    INSERT INTO sections (
                        course_id, course_name, group_id, group_name, classes_count,
                        lesson_type, subgroup_mode, subgroup_count, requires_computers,
                        teacher_id, teacher_name
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                inserted += 1

            generated_sections.append(
                {
                    "id": section_id,
                    "course_id": component["course_id"],
                    "course_name": component["course_name"],
                    "group_id": group["id"],
                    "group_name": group["name"],
                    "classes_count": classes_count,
                    "lesson_type": lesson_type,
                    "teacher_id": teacher_id,
                    "teacher_name": teacher_name or "",
                }
            )

    connection.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": 0,
        "missing": {"groups": False, "components": False},
        "sections": generated_sections,
    }


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


def normalize_specialty(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return normalized.upper() if normalized else ""


def normalize_programme(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return SPECIALTY_PROGRAMME_ALIASES.get(normalized, str(value or "").strip())


def infer_group_entry_year(group_name):
    match = re.search(r"\b05-057-(\d{2})-", str(group_name or ""))
    if not match:
        return None
    year_suffix = int(match.group(1))
    return 2000 + year_suffix


def current_academic_year_start():
    today = date.today()
    return today.year if today.month >= 9 else today.year - 1


def infer_study_course(entry_year):
    if not entry_year:
        return None
    return max(1, current_academic_year_start() - int(entry_year) + 1)


def normalize_subgroup(value):
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"A", "B"} else ""


def normalize_room_type(value):
    return str(value or "").strip().lower()


def schedule_room_type_matches(room_type, lesson_type, requires_computers=False):
    normalized_room_type = normalize_room_type(room_type)
    normalized_lesson_type = normalize_lesson_type(lesson_type)
    if normalized_lesson_type == "lecture":
        return normalized_room_type == "lecture"
    if normalized_lesson_type == "lab":
        return normalized_room_type == "lab"
    if normalized_lesson_type == "practical" and requires_computers:
        return True
    return normalized_room_type == "practical"


def schedule_student_count_for_room(section, group, subgroup):
    student_count = int(group.get("student_count") or 0)
    if not subgroup:
        return student_count

    subgroup_count = positive_int(section.get("subgroup_count"), 1)
    if group.get("has_subgroups"):
        subgroup_count = max(2, subgroup_count)
    return ceil(student_count / subgroup_count) if student_count else 0


def validate_schedule_payload(connection, payload, exclude_schedule_id=None):
    section_id = payload.get("section_id")
    room_id = payload.get("room_id")
    teacher_id = payload.get("teacher_id")
    day = payload.get("day")
    start_hour = payload.get("start_hour")
    subgroup = normalize_subgroup(payload.get("subgroup"))

    if not section_id or not room_id or not teacher_id or not day or start_hour in (None, ""):
        raise ApiError(400, "fill_required_fields", "Заполните поля расписания")

    section = query_one(
        connection,
        """
        SELECT
            s.id, s.course_id, s.course_name, s.group_id, s.group_name,
            s.lesson_type, s.subgroup_mode, s.subgroup_count, s.requires_computers,
            COALESCE(s.teacher_id, c.instructor_id) AS teacher_id,
            COALESCE(NULLIF(s.teacher_name, ''), c.instructor_name, '') AS teacher_name,
            c.year AS course_year,
            g.student_count, g.has_subgroups, g.study_course
        FROM sections s
        JOIN courses c ON c.id = s.course_id
        JOIN groups g ON g.id = s.group_id
        WHERE s.id = ?
        """,
        (section_id,),
    )
    if section is None:
        raise ApiError(400, "bad_request", "Для расписания не найдена секция")

    room = query_one(
        connection,
        """
        SELECT id, number, capacity, type, available, computer_count
        FROM rooms
        WHERE id = ?
        """,
        (room_id,),
    )
    if room is None or not int(room.get("available") if room.get("available") is not None else 1):
        raise ApiError(400, "bad_request", "Для расписания не найдена доступная аудитория")

    if int(payload.get("course_id") or section["course_id"]) != int(section["course_id"]):
        raise ApiError(400, "bad_request", "Дисциплина не совпадает с выбранной секцией")
    if int(payload.get("group_id") or section["group_id"]) != int(section["group_id"]):
        raise ApiError(400, "bad_request", "Группа не совпадает с выбранной секцией")
    if int(teacher_id) != int(section.get("teacher_id") or 0):
        raise ApiError(400, "bad_request", "Преподаватель не совпадает с выбранной секцией")
    if section.get("study_course") and int(section.get("study_course")) != int(section.get("course_year") or 0):
        raise ApiError(400, "bad_request", "Курс группы не совпадает с курсом дисциплины")

    lesson_type = normalize_lesson_type(section.get("lesson_type"))
    requires_computers = bool(section.get("requires_computers")) or lesson_type == "lab"
    if not schedule_room_type_matches(room.get("type"), lesson_type, requires_computers):
        raise ApiError(400, "bad_request", "Аудитория не подходит для типа занятия")

    effective_student_count = schedule_student_count_for_room(section, section, subgroup)
    room_capacity = int(room.get("capacity") or 0)
    if room_capacity and effective_student_count and room_capacity < effective_student_count:
        raise ApiError(400, "bad_request", "Вместимость аудитории меньше количества студентов")

    pc_count = int(room.get("computer_count") or 0)
    if requires_computers and (pc_count <= 0 or (effective_student_count and pc_count < effective_student_count)):
        raise ApiError(400, "bad_request", "В аудитории недостаточно компьютеров")

    exclude_clause = "AND id <> ?" if exclude_schedule_id is not None else ""
    exclude_params = [exclude_schedule_id] if exclude_schedule_id is not None else []

    room_conflict = query_one(
        connection,
        f"""
        SELECT id
        FROM schedules
        WHERE room_id = ? AND day = ? AND start_hour = ?
        {exclude_clause}
        LIMIT 1
        """,
        tuple([room_id, day, start_hour, *exclude_params]),
    )
    if room_conflict:
        raise ApiError(400, "bad_request", "Аудитория уже занята в это время")

    teacher_conflict = query_one(
        connection,
        f"""
        SELECT id
        FROM schedules
        WHERE teacher_id = ? AND day = ? AND start_hour = ?
        {exclude_clause}
        LIMIT 1
        """,
        tuple([teacher_id, day, start_hour, *exclude_params]),
    )
    if teacher_conflict:
        raise ApiError(400, "bad_request", "Преподаватель уже занят в это время")

    group_conflict = query_one(
        connection,
        f"""
        SELECT id
        FROM schedules
        WHERE group_id = ? AND day = ? AND start_hour = ?
          AND (coalesce(subgroup, '') = '' OR ? = '' OR upper(subgroup) = ?)
        {exclude_clause}
        LIMIT 1
        """,
        tuple([section["group_id"], day, start_hour, subgroup, subgroup, *exclude_params]),
    )
    if group_conflict:
        raise ApiError(400, "bad_request", "Группа или подгруппа уже занята в это время")


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
                year, semester, department, instructor_id, instructor_name, programme,
                module_type, module_name, cycle, component, language, academic_year, entry_year,
                requires_computers
            FROM courses
            ORDER BY id
            """,
        )

    if collection == "course_components":
        clauses = []
        params = []
        course_id = query.get("course_id", [None])[0]
        academic_period = query.get("academic_period", [None])[0]
        if course_id is not None:
            clauses.append("course_id = ?")
            params.append(course_id)
        if academic_period is not None:
            clauses.append("academic_period = ?")
            params.append(academic_period)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return query_all(
            connection,
            f"""
            SELECT
                id, course_id, course_code, course_name, programme, study_year,
                academic_period, semester, lesson_type, hours, weekly_classes,
                requires_computers, teacher_id, teacher_name
            FROM course_components
            {where_sql}
            ORDER BY academic_period, course_name, lesson_type, id
            """,
            tuple(params),
        )

    if collection == "iup_entries":
        return query_all(
            connection,
            """
            SELECT
                id, file_name, group_name, programme, study_course,
                language, academic_year, academic_period, semester, component,
                course_code, course_name, credits, lesson_type, teacher_id,
                teacher_name, hours
            FROM iup_entries
            ORDER BY file_name, academic_period, course_name, lesson_type, id
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
            SELECT id, name, student_count, has_subgroups, language, programme, specialty_code, entry_year, study_course
            FROM groups
            ORDER BY id
            """,
        )

    if collection == "sections":
        return query_all(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type, subgroup_mode, subgroup_count, requires_computers, teacher_id, teacher_name
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
        normalized = normalize_number_fields(payload, ["credits", "hours", "year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO courses (
                name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme,
                module_type, module_name, cycle, component, language, academic_year, entry_year,
                requires_computers
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                course_name,
                course_code,
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
                normalized.get("year", normalized.get("study_year")),
                normalized.get("semester"),
                normalized.get("department", ""),
                normalized.get("instructor_id"),
                normalized.get("instructor_name", ""),
                normalized.get("programme", normalized.get("programme_name", "")),
                normalized.get("module_type", ""),
                normalized.get("module_name", ""),
                normalized.get("cycle", ""),
                normalized.get("component", ""),
                normalized.get("language", ""),
                normalized.get("academic_year", ""),
                normalized.get("entry_year", ""),
                1 if normalized.get("requires_computers", 0) else 0,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme,
                module_type, module_name, cycle, component, language, academic_year, entry_year,
                requires_computers
            FROM courses
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "course_components":
        normalized = normalize_number_fields(
            payload,
            [
                "course_id",
                "study_year",
                "academic_period",
                "semester",
                "hours",
                "weekly_classes",
                "requires_computers",
                "teacher_id",
            ],
        )
        lesson_type = normalize_lesson_type(normalized.get("lesson_type"))
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO course_components (
                course_id, course_code, course_name, programme, study_year,
                academic_period, semester, lesson_type, hours, weekly_classes,
                requires_computers, teacher_id, teacher_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_code", ""),
                normalized.get("course_name", ""),
                normalized.get("programme", ""),
                normalized.get("study_year"),
                normalized.get("academic_period"),
                normalized.get("semester"),
                lesson_type,
                normalized.get("hours"),
                normalized.get("weekly_classes"),
                section_requires_computers(
                    lesson_type,
                    normalized.get("course_code", ""),
                    normalized.get("course_name", ""),
                    normalized.get("study_year"),
                ),
                normalized.get("teacher_id"),
                normalized.get("teacher_name", ""),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_code, course_name, programme, study_year,
                academic_period, semester, lesson_type, hours, weekly_classes,
                requires_computers, teacher_id, teacher_name
            FROM course_components
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
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "entry_year", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        specialty_code = normalize_specialty(normalized.get("specialty_code", normalized.get("specialty", "")))
        programme = normalized.get("programme") or normalize_programme(specialty_code)
        entry_year = normalized.get("entry_year") or infer_group_entry_year(normalized.get("name"))
        study_course = normalized.get("study_course") or infer_study_course(entry_year)
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO groups (name, student_count, has_subgroups, language, programme, specialty_code, entry_year, study_course)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("student_count") or 0,
                1 if normalized.get("has_subgroups", 0) else 0,
                group_language,
                programme,
                specialty_code,
                entry_year,
                study_course,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, student_count, has_subgroups, language, programme, specialty_code, entry_year, study_course
            FROM groups
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "sections":
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count", "subgroup_count", "requires_computers", "teacher_id"])
        normalized["lesson_type"] = normalize_lesson_type(normalized.get("lesson_type"))
        normalized["subgroup_mode"] = normalize_subgroup_mode(normalized.get("subgroup_mode"), normalized["lesson_type"])
        normalized["subgroup_count"] = positive_int(normalized.get("subgroup_count"), 1)
        if "requires_computers" in normalized:
            requires_computers = 1 if normalized.get("requires_computers") else 0
        else:
            requires_computers = section_requires_computers(normalized["lesson_type"])
        teacher_id, teacher_name = resolve_section_teacher(
            connection,
            normalized.get("course_id"),
            normalized["lesson_type"],
            normalized,
        )
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO sections (course_id, course_name, group_id, group_name, classes_count, lesson_type, subgroup_mode, subgroup_count, requires_computers, teacher_id, teacher_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("group_id"),
                normalized.get("group_name", ""),
                normalized.get("classes_count", normalized.get("class_count")),
                normalized["lesson_type"],
                normalized["subgroup_mode"],
                normalized["subgroup_count"],
                requires_computers,
                teacher_id,
                teacher_name,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type, subgroup_mode, subgroup_count, requires_computers, teacher_id, teacher_name
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
        normalized["subgroup"] = normalize_subgroup(normalized.get("subgroup"))
        validate_schedule_payload(connection, normalized)
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
        normalized = normalize_number_fields(payload, ["credits", "hours", "year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        db_execute(
            connection,
            """
            UPDATE courses
            SET
                name = ?, code = ?, credits = ?, hours = ?, description = ?,
                year = ?, semester = ?, department = ?, instructor_id = ?, instructor_name = ?,
                programme = ?, module_type = ?, module_name = ?, cycle = ?, component = ?,
                language = ?, academic_year = ?, entry_year = ?, requires_computers = ?
            WHERE id = ?
            """,
            (
                course_name,
                course_code,
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
                normalized.get("year", normalized.get("study_year")),
                normalized.get("semester"),
                normalized.get("department", ""),
                normalized.get("instructor_id"),
                normalized.get("instructor_name", ""),
                normalized.get("programme", normalized.get("programme_name", "")),
                normalized.get("module_type", ""),
                normalized.get("module_name", ""),
                normalized.get("cycle", ""),
                normalized.get("component", ""),
                normalized.get("language", ""),
                normalized.get("academic_year", ""),
                normalized.get("entry_year", ""),
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
                year, semester, department, instructor_id, instructor_name, programme,
                module_type, module_name, cycle, component, language, academic_year, entry_year,
                requires_computers
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
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "entry_year", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        specialty_code = normalize_specialty(normalized.get("specialty_code", normalized.get("specialty", "")))
        programme = normalized.get("programme") or normalize_programme(specialty_code)
        entry_year = normalized.get("entry_year") or infer_group_entry_year(normalized.get("name"))
        study_course = normalized.get("study_course") or infer_study_course(entry_year)
        db_execute(
            connection,
            """
            UPDATE groups
            SET name = ?, student_count = ?, has_subgroups = ?, language = ?, programme = ?, specialty_code = ?, entry_year = ?, study_course = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("student_count") or 0,
                1 if normalized.get("has_subgroups", 0) else 0,
                group_language,
                programme,
                specialty_code,
                entry_year,
                study_course,
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, student_count, has_subgroups, language, programme, specialty_code, entry_year, study_course
            FROM groups
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "sections":
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count", "subgroup_count", "requires_computers", "teacher_id"])
        normalized["lesson_type"] = normalize_lesson_type(normalized.get("lesson_type"))
        normalized["subgroup_mode"] = normalize_subgroup_mode(normalized.get("subgroup_mode"), normalized["lesson_type"])
        normalized["subgroup_count"] = positive_int(normalized.get("subgroup_count"), 1)
        if "requires_computers" in normalized:
            requires_computers = 1 if normalized.get("requires_computers") else 0
        else:
            requires_computers = section_requires_computers(normalized["lesson_type"])
        teacher_id, teacher_name = resolve_section_teacher(
            connection,
            normalized.get("course_id"),
            normalized["lesson_type"],
            normalized,
        )
        db_execute(
            connection,
            """
            UPDATE sections
            SET course_id = ?, course_name = ?, group_id = ?, group_name = ?, classes_count = ?, lesson_type = ?, subgroup_mode = ?, subgroup_count = ?, requires_computers = ?, teacher_id = ?, teacher_name = ?
            WHERE id = ?
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("group_id"),
                normalized.get("group_name", ""),
                normalized.get("classes_count", normalized.get("class_count")),
                normalized["lesson_type"],
                normalized["subgroup_mode"],
                normalized["subgroup_count"],
                requires_computers,
                teacher_id,
                teacher_name,
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, course_id, course_name, group_id, group_name, classes_count, lesson_type, subgroup_mode, subgroup_count, requires_computers, teacher_id, teacher_name
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
        normalized["subgroup"] = normalize_subgroup(normalized.get("subgroup"))
        validate_schedule_payload(connection, normalized, exclude_schedule_id=item_id)
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
        course = query_one(connection, "SELECT code FROM courses WHERE id = ?", (item_id,))
        db_execute(connection, "DELETE FROM schedules WHERE course_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM sections WHERE course_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM course_components WHERE course_id = ?", (item_id,))
        if course:
            db_execute(connection, "DELETE FROM iup_entries WHERE lower(course_code) = lower(?)", (course["code"],))
    elif collection == "groups":
        group = query_one(connection, "SELECT name FROM groups WHERE id = ?", (item_id,))
        db_execute(connection, "DELETE FROM schedules WHERE group_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM sections WHERE group_id = ?", (item_id,))
        db_execute(connection, "UPDATE students SET group_id = NULL, group_name = '', subgroup = '' WHERE group_id = ?", (item_id,))
        if group:
            db_execute(connection, "DELETE FROM iup_entries WHERE group_name = ?", (group["name"],))
    elif collection == "teachers":
        db_execute(connection, "DELETE FROM schedules WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "UPDATE courses SET instructor_id = NULL, instructor_name = '' WHERE instructor_id = ?", (item_id,))
        db_execute(connection, "UPDATE course_components SET teacher_id = NULL, teacher_name = '' WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "UPDATE sections SET teacher_id = NULL, teacher_name = '' WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM teacher_preference_requests WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'teacher' AND recipient_id = ?", (item_id,))
    elif collection == "rooms":
        db_execute(connection, "DELETE FROM schedules WHERE room_id = ?", (item_id,))
    elif collection == "students":
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'student' AND recipient_id = ?", (item_id,))
    db_execute(connection, f"DELETE FROM {collection} WHERE id = ?", (item_id,))
    connection.commit()
