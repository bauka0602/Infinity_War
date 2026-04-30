import re
from copy import deepcopy
from datetime import date
from math import ceil

from .config import DB_ENGINE, TEACHER_EMAIL_DOMAIN
from .db import db_execute, insert_and_get_id, query_all, query_one
from .education_programmes import (
    get_home_room_programmes,
    resolve_education_group_value,
    room_matches_home_programmes,
)
from .errors import ApiError
from .lesson_rules import requires_computers_for_component
from .notification_service import create_schedule_change_notifications
from .programme_utils import same_programme
from .room_availability import (
    get_room_blocked_slots,
    normalize_room_block_day,
    recompute_room_availability,
)
from .teacher_utils import build_teacher_name_signature, normalize_teacher_name

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

GROUP_SUBGROUPS_AGGREGATE_SQL = (
    """
    COALESCE(
        (
            SELECT string_agg(subgroup_value, ',' ORDER BY subgroup_value)
            FROM (
                SELECT DISTINCT upper(trim(s.subgroup)) AS subgroup_value
                FROM schedules s
                WHERE s.group_id = g.id
                  AND trim(coalesce(s.subgroup, '')) <> ''
            ) subgroup_values
        ),
        ''
    ) AS generated_subgroups
    """
    if DB_ENGINE == "postgres"
    else
    """
    COALESCE(
        (
            SELECT group_concat(subgroup_value, ',')
            FROM (
                SELECT DISTINCT upper(trim(s.subgroup)) AS subgroup_value
                FROM schedules s
                WHERE s.group_id = g.id
                  AND trim(coalesce(s.subgroup, '')) <> ''
                ORDER BY subgroup_value
            )
        ),
        ''
    ) AS generated_subgroups
    """
)

SPECIALTY_PROGRAMME_ALIASES = {
    "би": "Бизнес-информатика",
    "бизи": "Бизнес-информатика",
    "ки": "Компьютерная инженерия",
    "ки сопр": "Компьютерная инженерия (СОПР)",
    "сопр": "Компьютерная инженерия (СОПР)",
}
MIN_COMPUTER_COUNT = 10


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


def normalize_room_block_interval(payload):
    normalized = normalize_number_fields(payload, ["room_id", "start_hour", "end_hour", "semester", "year"])
    day = str(normalized.get("day") or "").strip()
    start_hour = normalized.get("start_hour")
    end_hour = normalized.get("end_hour")
    if not normalized.get("room_id") or not day or start_hour in (None, ""):
        raise ApiError(400, "fill_required_fields", "Заполните поля: room_id, day, start_hour")
    if end_hour in (None, ""):
        end_hour = int(start_hour) + 1
    if int(end_hour) <= int(start_hour):
        raise ApiError(400, "bad_request", "В блокировке аудитории end_hour должен быть больше start_hour")
    normalized["day"] = day
    normalized["start_hour"] = int(start_hour)
    normalized["end_hour"] = int(end_hour)
    return normalized


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


def _teacher_disciplines_map(connection):
    rows = query_all(
        connection,
        """
        SELECT teacher_id, discipline_name
        FROM (
            SELECT teacher_id, course_name AS discipline_name
            FROM course_components
            WHERE teacher_id IS NOT NULL
              AND trim(coalesce(course_name, '')) <> ''

            UNION

            SELECT instructor_id AS teacher_id, name AS discipline_name
            FROM courses
            WHERE instructor_id IS NOT NULL
              AND trim(coalesce(name, '')) <> ''

            UNION

            SELECT teacher_id, course_name AS discipline_name
            FROM sections
            WHERE teacher_id IS NOT NULL
              AND trim(coalesce(course_name, '')) <> ''
        ) assigned
        ORDER BY teacher_id, discipline_name
        """,
    )
    disciplines_by_teacher = {}
    for row in rows:
        teacher_id = row.get("teacher_id")
        discipline_name = str(row.get("discipline_name") or "").strip()
        if not teacher_id or not discipline_name:
            continue
        disciplines = disciplines_by_teacher.setdefault(teacher_id, [])
        if discipline_name not in disciplines:
            disciplines.append(discipline_name)
    return disciplines_by_teacher


def _serialize_teacher(row, disciplines_by_teacher=None):
    disciplines = list((disciplines_by_teacher or {}).get(row["id"], []))
    return {
        **row,
        "assigned_disciplines": disciplines,
        "assigned_disciplines_text": ", ".join(disciplines),
        "assigned_disciplines_count": len(disciplines),
    }


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
    return same_programme(left, right)


def _same_education_group(left_programme="", left_specialty="", right_programme="", right_specialty=""):
    left_group = resolve_education_group_value(left_programme, left_specialty)
    right_group = resolve_education_group_value(right_programme, right_specialty)
    if left_group and right_group:
        return left_group == right_group
    return _same_programme(left_programme, right_programme)


def generate_sections_from_components(connection, payload):
    semester = payload.get("semester")
    study_course = payload.get("study_course") or payload.get("year")
    programme = str(payload.get("programme") or "").strip()

    semester = int(semester) if semester else None
    study_course = int(study_course) if study_course else None
    all_groups = query_all(
        connection,
        """
        SELECT id, name, programme, specialty_code, study_course
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
        and (
            not programme
            or _same_education_group(group.get("programme"), group.get("specialty_code"), programme, "")
        )
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
        if (not programme or _same_education_group(component.get("programme"), "", programme, ""))
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
            if _same_education_group(
                group.get("programme"),
                group.get("specialty_code"),
                component.get("programme"),
                "",
            )
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
    if normalized_lesson_type == "practical":
        return normalized_room_type in {"practical", "lecture"}
    if normalized_lesson_type == "lab":
        return normalized_room_type == "practical"
    return normalized_room_type == "practical"


def schedule_student_count_for_room(section, group, subgroup):
    student_count = int(group.get("student_count") or 0)
    if not subgroup:
        return student_count

    return ceil(student_count / 2) if student_count else 0


def _room_candidate_score(room, schedule_row):
    home_programmes = get_home_room_programmes(
        schedule_row.get("group_programme"),
        schedule_row.get("specialty_code"),
    )
    room_programme = room.get("programme") or ""
    score = 0
    lesson_type = normalize_lesson_type(schedule_row.get("lesson_type"))
    room_type = normalize_room_type(room.get("type"))
    if lesson_type == "practical":
        if room_type == "practical":
            score += 45
            if int(room.get("computer_count") or 0) >= MIN_COMPUTER_COUNT:
                score += 8
        elif room_type == "lecture":
            score -= 12
    elif lesson_type == "lecture" and room_type == "lecture":
        score += 35
    elif lesson_type == "lab" and room_type == "practical":
        score += 45
    if home_programmes and room_programme:
        if any(same_programme(room_programme, home_programme) for home_programme in home_programmes):
            score += 100
        else:
            score -= 20
    if room.get("capacity") and schedule_row.get("effective_student_count"):
        score -= abs(int(room.get("capacity") or 0) - int(schedule_row.get("effective_student_count") or 0))
    return score


def _find_alternative_room_for_schedule(connection, schedule_row, blocked_slots_by_room=None, excluded_room_ids=None):
    blocked_slots_by_room = blocked_slots_by_room or {}
    excluded_room_ids = set(excluded_room_ids or [])
    rooms = query_all(
        connection,
        """
        SELECT id, number, capacity, type, available, computer_count, programme
        FROM rooms
        WHERE coalesce(available, 1) = 1
        ORDER BY id
        """,
    )
    normalized_day = normalize_room_block_day(schedule_row.get("day"))
    lesson_type = normalize_lesson_type(schedule_row.get("lesson_type"))
    requires_computers = bool(schedule_row.get("requires_computers")) or lesson_type == "lab"
    candidates = []
    for room in rooms:
        if room["id"] == schedule_row.get("room_id") or room["id"] in excluded_room_ids:
            continue
        if not schedule_room_type_matches(room.get("type"), lesson_type, requires_computers):
            continue
        if int(room.get("capacity") or 0) < int(schedule_row.get("effective_student_count") or 0):
            continue
        if requires_computers and int(room.get("computer_count") or 0) < MIN_COMPUTER_COUNT:
            continue
        if query_one(
            connection,
            """
            SELECT id
            FROM schedules
            WHERE room_id = ? AND day = ? AND start_hour = ? AND id <> ?
            LIMIT 1
            """,
            (room["id"], schedule_row.get("day"), schedule_row.get("start_hour"), schedule_row["id"]),
        ):
            continue
        if (normalized_day, int(schedule_row.get("start_hour") or 0)) in blocked_slots_by_room.get(room["id"], set()):
            continue
        candidates.append(room)
    if not candidates:
        return None
    candidates.sort(key=lambda room: _room_candidate_score(room, schedule_row), reverse=True)
    return candidates[0]


def _relocate_conflicting_room_schedules(connection, room_block, exclude_block_id=None):
    blocked_slots_by_room = get_room_blocked_slots(connection, room_block.get("semester"), room_block.get("year"))
    conflicting_schedules = [
        row
        for row in query_all(
            connection,
            """
            SELECT
                sc.id,
                sc.section_id,
                sc.course_id,
                sc.course_name,
                sc.teacher_id,
                sc.teacher_name,
                sc.room_id,
                sc.room_number,
                sc.room_programme,
                sc.room_programme_mismatch,
                sc.day,
                sc.start_hour,
                sc.semester,
                sc.year,
                sc.group_id,
                sc.group_name,
                sc.subgroup,
                sec.lesson_type,
                sec.subgroup_count,
                sec.requires_computers,
                grp.student_count,
                grp.has_subgroups,
                grp.programme AS group_programme,
                grp.specialty_code
            FROM schedules sc
            JOIN sections sec ON sec.id = sc.section_id
            JOIN groups grp ON grp.id = sc.group_id
            WHERE sc.room_id = ?
            """,
            (room_block.get("room_id"),),
        )
        if (room_block.get("semester") in (None, "") or row.get("semester") == room_block.get("semester"))
        and (room_block.get("year") in (None, "") or row.get("year") == room_block.get("year"))
        and normalize_room_block_day(row.get("day")) == normalize_room_block_day(room_block.get("day"))
        and int(room_block.get("start_hour") or 0) <= int(row.get("start_hour") or 0) < int(room_block.get("end_hour") or 0)
    ]
    relocated = []
    for schedule_row in conflicting_schedules:
        schedule_row["effective_student_count"] = schedule_student_count_for_room(
            schedule_row,
            schedule_row,
            normalize_subgroup(schedule_row.get("subgroup")),
        )
        alternative_room = _find_alternative_room_for_schedule(
            connection,
            schedule_row,
            blocked_slots_by_room=blocked_slots_by_room,
            excluded_room_ids={room_block.get("room_id")},
        )
        if alternative_room is None:
            raise ApiError(
                400,
                "bad_request",
                "Не удалось перенести одно из конфликтующих занятий в другую аудиторию.",
                details={
                    "scheduleId": schedule_row["id"],
                    "groupName": schedule_row.get("group_name"),
                    "day": schedule_row.get("day"),
                    "startHour": schedule_row.get("start_hour"),
                },
            )
        room_programme, room_programme_mismatch = resolve_schedule_room_programme_meta(
            connection,
            schedule_row["section_id"],
            alternative_room["id"],
        )
        db_execute(
            connection,
            """
            UPDATE schedules
            SET
                room_id = ?,
                room_number = ?,
                room_programme = ?,
                room_programme_mismatch = ?,
                relocated_from_room_number = ?,
                relocation_reason = ?
            WHERE id = ?
            """,
            (
                alternative_room["id"],
                alternative_room.get("number", ""),
                room_programme,
                room_programme_mismatch,
                schedule_row.get("room_number", ""),
                room_block.get("reason", "") or "room_block",
                schedule_row["id"],
            ),
        )
        before_schedule = {
            **schedule_row,
            "room_programme": schedule_row.get("room_programme", ""),
            "room_programme_mismatch": schedule_row.get("room_programme_mismatch", 0),
            "relocated_from_room_number": "",
            "relocation_reason": "",
        }
        after_schedule = {
            **before_schedule,
            "room_id": alternative_room["id"],
            "room_number": alternative_room.get("number", ""),
            "room_programme": room_programme,
            "room_programme_mismatch": room_programme_mismatch,
            "relocated_from_room_number": schedule_row.get("room_number", ""),
            "relocation_reason": room_block.get("reason", "") or "room_block",
        }
        create_schedule_change_notifications(connection, before_item=before_schedule, after_item=after_schedule)
        relocated.append(
            {
                "scheduleId": schedule_row["id"],
                "fromRoomId": schedule_row.get("room_id"),
                "fromRoomNumber": schedule_row.get("room_number"),
                "toRoomId": alternative_room["id"],
                "toRoomNumber": alternative_room.get("number", ""),
                "day": schedule_row.get("day"),
                "startHour": schedule_row.get("start_hour"),
                "groupName": schedule_row.get("group_name"),
                "relocationReason": room_block.get("reason", "") or "room_block",
            }
        )
    return relocated


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
            c.programme AS course_programme,
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
        SELECT id, number, capacity, type, available, computer_count, programme
        FROM rooms
        WHERE id = ?
        """,
        (room_id,),
    )
    if room is None:
        raise ApiError(400, "bad_request", "Для расписания не найдена аудитория")
    if not int(room.get("available") or 0):
        raise ApiError(400, "bad_request", "Аудитория отключена и недоступна для расписания")

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
    if requires_computers and pc_count < MIN_COMPUTER_COUNT:
        raise ApiError(
            400,
            "bad_request",
            f"Для этого занятия в аудитории должно быть минимум {MIN_COMPUTER_COUNT} компьютеров",
        )

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

    room_blocked_slots = get_room_blocked_slots(
        connection,
        payload.get("semester"),
        payload.get("year"),
    )
    if (normalize_room_block_day(day), int(start_hour)) in room_blocked_slots.get(room_id, set()):
        raise ApiError(400, "bad_request", "Аудитория недоступна в этот временной слот")

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
        teachers = query_all(
            connection,
            """
            SELECT id, name, email, phone, subject_taught, weekly_hours_limit, teaching_languages
            FROM teachers
            ORDER BY id
            """,
        )
        disciplines_by_teacher = _teacher_disciplines_map(connection)
        return [_serialize_teacher(row, disciplines_by_teacher) for row in teachers]

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
            SELECT
                id,
                number,
                capacity,
                '' AS building,
                type,
                equipment,
                programme,
                available,
                computer_count
            FROM rooms
            ORDER BY id
            """,
        )

    if collection == "room_blocks":
        return query_all(
            connection,
            """
            SELECT id, room_id, day, start_hour, end_hour, semester, year, reason
            FROM room_blocks
            ORDER BY room_id, day, start_hour, id
            """,
        )

    if collection == "groups":
        return query_all(
            connection,
            """
            SELECT
                g.id,
                g.name,
                g.student_count,
                g.has_subgroups,
                g.language,
                g.programme,
                g.specialty_code,
                g.entry_year,
                g.study_course,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM schedules s
                        WHERE s.group_id = g.id
                          AND trim(coalesce(s.subgroup, '')) <> ''
                    )
                    THEN 1
                    ELSE 0
                END AS auto_has_subgroups,
                {GROUP_SUBGROUPS_AGGREGATE_SQL}
            FROM groups g
            ORDER BY g.id
            """.format(GROUP_SUBGROUPS_AGGREGATE_SQL=GROUP_SUBGROUPS_AGGREGATE_SQL),
        )

    if collection == "sections":
        return query_all(
            connection,
            """
            SELECT
                id, course_id, course_name, group_id, group_name, classes_count,
                lesson_type, subgroup_mode, subgroup_count, requires_computers,
                teacher_id, teacher_name, iup_entry_id, source, match_method
            FROM sections
            ORDER BY id
            """,
        )

    clauses = []
    params = []
    semester = query.get("semester", [None])[0]
    year = query.get("year", [None])[0]
    from_sql = "FROM schedules s LEFT JOIN sections sec ON sec.id = s.section_id"
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
            s.group_id, s.group_name, s.subgroup, s.day, s.start_hour, s.semester, s.year, s.algorithm,
            COALESCE(sec.lesson_type, 'lecture') AS lesson_type,
            s.room_programme, s.room_programme_mismatch, s.relocated_from_room_number, s.relocation_reason
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
            INSERT INTO teachers (
                name, email, phone, subject_taught, weekly_hours_limit, teaching_languages,
                name_normalized, name_signature
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("subject_taught", normalized.get("department", normalized.get("specialization", ""))),
                normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages,
                normalize_teacher_name(normalized.get("name")),
                build_teacher_name_signature(normalized.get("name")),
            ),
        )
        connection.commit()
        teacher = query_one(
            connection,
            """
            SELECT id, name, email, phone, subject_taught, weekly_hours_limit, teaching_languages
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )
        return _serialize_teacher(teacher, _teacher_disciplines_map(connection))

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO rooms (number, capacity, type, equipment, programme, available, computer_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                normalized.get("programme", normalized.get("department", "")),
                1 if normalized.get("available", normalized.get("is_available", 1)) else 0,
                normalized.get("computer_count", 0),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id,
                number,
                capacity,
                '' AS building,
                type,
                equipment,
                programme,
                available,
                computer_count
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "room_blocks":
        normalized = normalize_room_block_interval(payload)
        relocated = _relocate_conflicting_room_schedules(connection, normalized)
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO room_blocks (room_id, day, start_hour, end_hour, semester, year, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("room_id"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("end_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("reason", ""),
            ),
        )
        connection.commit()
        row = query_one(
            connection,
            """
            SELECT id, room_id, day, start_hour, end_hour, semester, year, reason
            FROM room_blocks
            WHERE id = ?
            """,
            (item_id,),
        )
        return {**row, "relocatedSchedules": relocated}

    if collection == "groups":
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "entry_year", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        specialty_code = normalize_specialty(normalized.get("specialty_code", normalized.get("specialty", "")))
        programme = resolve_education_group_value(
            normalized.get("programme"),
            specialty_code,
        ) or normalize_programme(specialty_code)
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
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count", "subgroup_count", "requires_computers", "teacher_id", "iup_entry_id"])
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
            INSERT INTO sections (
                course_id, course_name, group_id, group_name, classes_count,
                lesson_type, subgroup_mode, subgroup_count, requires_computers,
                teacher_id, teacher_name, iup_entry_id, source, match_method
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                normalized.get("iup_entry_id"),
                normalized.get("source", "manual"),
                normalized.get("match_method", "manual"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, group_id, group_name, classes_count,
                lesson_type, subgroup_mode, subgroup_count, requires_computers,
                teacher_id, teacher_name, iup_entry_id, source, match_method
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
        room_programme, room_programme_mismatch = resolve_schedule_room_programme_meta(
            connection,
            normalized.get("section_id"),
            normalized.get("room_id"),
        )
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO schedules (
                section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
                room_programme, room_programme_mismatch, relocated_from_room_number, relocation_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                room_programme,
                room_programme_mismatch,
                normalized.get("relocated_from_room_number", ""),
                normalized.get("relocation_reason", ""),
            ),
        )
        recompute_room_availability(connection)
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
                room_programme, room_programme_mismatch, relocated_from_room_number, relocation_reason
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def resolve_schedule_room_programme_meta(connection, section_id, room_id):
    row = query_one(
        connection,
        """
        SELECT
            c.programme AS course_programme,
            g.programme AS group_programme,
            g.specialty_code AS specialty_code,
            r.programme AS room_programme
        FROM sections s
        JOIN courses c ON c.id = s.course_id
        JOIN groups g ON g.id = s.group_id
        JOIN rooms r ON r.id = ?
        WHERE s.id = ?
        """,
        (room_id, section_id),
    )
    if not row:
        return "", 0

    course_programme = row.get("course_programme") or ""
    group_programme = row.get("group_programme") or ""
    specialty_code = row.get("specialty_code") or ""
    room_programme = row.get("room_programme") or ""
    home_match = room_matches_home_programmes(
        room_programme,
        group_programme,
        specialty_code,
    )
    if group_programme or specialty_code:
        mismatch = bool(room_programme and not home_match)
    else:
        mismatch = bool(
            course_programme
            and room_programme
            and not same_programme(course_programme, room_programme)
        )
    return room_programme, 1 if mismatch else 0


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
        recompute_room_availability(connection)
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
            SET
                name = ?,
                email = ?,
                phone = ?,
                subject_taught = ?,
                weekly_hours_limit = ?,
                teaching_languages = ?,
                name_normalized = ?,
                name_signature = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("subject_taught", normalized.get("department", normalized.get("specialization", ""))),
                normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages,
                normalize_teacher_name(normalized.get("name")),
                build_teacher_name_signature(normalized.get("name")),
                item_id,
            ),
        )
        connection.commit()
        teacher = query_one(
            connection,
            """
            SELECT id, name, email, phone, subject_taught, weekly_hours_limit, teaching_languages
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )
        return _serialize_teacher(teacher, _teacher_disciplines_map(connection))

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        db_execute(
            connection,
            """
            UPDATE rooms
            SET number = ?, capacity = ?, type = ?, equipment = ?, programme = ?, available = ?, computer_count = ?
            WHERE id = ?
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                normalized.get("programme", normalized.get("department", "")),
                1 if normalized.get("available", normalized.get("is_available", 1)) else 0,
                normalized.get("computer_count", 0),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id,
                number,
                capacity,
                '' AS building,
                type,
                equipment,
                programme,
                available,
                computer_count
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "room_blocks":
        normalized = normalize_room_block_interval(payload)
        relocated = _relocate_conflicting_room_schedules(connection, normalized, exclude_block_id=item_id)
        db_execute(
            connection,
            """
            UPDATE room_blocks
            SET room_id = ?, day = ?, start_hour = ?, end_hour = ?, semester = ?, year = ?, reason = ?
            WHERE id = ?
            """,
            (
                normalized.get("room_id"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("end_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("reason", ""),
                item_id,
            ),
        )
        connection.commit()
        row = query_one(
            connection,
            """
            SELECT id, room_id, day, start_hour, end_hour, semester, year, reason
            FROM room_blocks
            WHERE id = ?
            """,
            (item_id,),
        )
        return {**row, "relocatedSchedules": relocated}

    if collection == "groups":
        normalized = normalize_number_fields(payload, ["student_count", "has_subgroups", "entry_year", "study_course"])
        group_language = normalize_language(normalized.get("language"), "ru")
        specialty_code = normalize_specialty(normalized.get("specialty_code", normalized.get("specialty", "")))
        programme = resolve_education_group_value(
            normalized.get("programme"),
            specialty_code,
        ) or normalize_programme(specialty_code)
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
        normalized = normalize_number_fields(payload, ["course_id", "group_id", "classes_count", "class_count", "subgroup_count", "requires_computers", "teacher_id", "iup_entry_id"])
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
            SET
                course_id = ?, course_name = ?, group_id = ?, group_name = ?,
                classes_count = ?, lesson_type = ?, subgroup_mode = ?, subgroup_count = ?,
                requires_computers = ?, teacher_id = ?, teacher_name = ?,
                iup_entry_id = ?, source = ?, match_method = ?
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
                normalized.get("iup_entry_id"),
                normalized.get("source", "manual"),
                normalized.get("match_method", "manual"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, group_id, group_name, classes_count,
                lesson_type, subgroup_mode, subgroup_count, requires_computers,
                teacher_id, teacher_name, iup_entry_id, source, match_method
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
        room_programme, room_programme_mismatch = resolve_schedule_room_programme_meta(
            connection,
            normalized.get("section_id"),
            normalized.get("room_id"),
        )
        db_execute(
            connection,
            """
            UPDATE schedules
            SET
                section_id = ?, course_id = ?, course_name = ?, teacher_id = ?, teacher_name = ?,
                room_id = ?, room_number = ?, group_id = ?, group_name = ?, subgroup = ?,
                day = ?, start_hour = ?, semester = ?, year = ?, algorithm = ?,
                room_programme = ?, room_programme_mismatch = ?, relocated_from_room_number = ?, relocation_reason = ?
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
                room_programme,
                room_programme_mismatch,
                normalized.get("relocated_from_room_number", ""),
                normalized.get("relocation_reason", ""),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
                room_programme, room_programme_mismatch, relocated_from_room_number, relocation_reason
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def delete_collection_item(connection, collection, item_id):
    schedules_changed = False
    if collection == "courses":
        course = query_one(connection, "SELECT code FROM courses WHERE id = ?", (item_id,))
        db_execute(connection, "DELETE FROM schedules WHERE course_id = ?", (item_id,))
        schedules_changed = True
        db_execute(connection, "DELETE FROM sections WHERE course_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM course_components WHERE course_id = ?", (item_id,))
        if course:
            db_execute(connection, "DELETE FROM iup_entries WHERE lower(course_code) = lower(?)", (course["code"],))
    elif collection == "groups":
        group = query_one(connection, "SELECT name FROM groups WHERE id = ?", (item_id,))
        db_execute(connection, "DELETE FROM schedules WHERE group_id = ?", (item_id,))
        schedules_changed = True
        db_execute(connection, "DELETE FROM sections WHERE group_id = ?", (item_id,))
        db_execute(connection, "UPDATE students SET group_id = NULL, group_name = '', subgroup = '' WHERE group_id = ?", (item_id,))
        if group:
            db_execute(connection, "DELETE FROM iup_entries WHERE group_name = ?", (group["name"],))
    elif collection == "teachers":
        db_execute(connection, "DELETE FROM schedules WHERE teacher_id = ?", (item_id,))
        schedules_changed = True
        db_execute(connection, "UPDATE courses SET instructor_id = NULL, instructor_name = '' WHERE instructor_id = ?", (item_id,))
        db_execute(connection, "UPDATE course_components SET teacher_id = NULL, teacher_name = '' WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "UPDATE sections SET teacher_id = NULL, teacher_name = '' WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM teacher_preference_requests WHERE teacher_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'teacher' AND recipient_id = ?", (item_id,))
    elif collection == "rooms":
        db_execute(connection, "DELETE FROM schedules WHERE room_id = ?", (item_id,))
        db_execute(connection, "DELETE FROM room_blocks WHERE room_id = ?", (item_id,))
        schedules_changed = True
    elif collection == "room_blocks":
        pass
    elif collection == "students":
        db_execute(connection, "DELETE FROM notifications WHERE recipient_role = 'student' AND recipient_id = ?", (item_id,))
    db_execute(connection, f"DELETE FROM {collection} WHERE id = ?", (item_id,))
    if schedules_changed and collection != "rooms":
        recompute_room_availability(connection)
    connection.commit()
