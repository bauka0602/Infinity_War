from calendar import monthrange
from datetime import date, timedelta
from math import ceil

from .db import db_execute, db_executemany, query_all
from .errors import ApiError
from .optimizer import optimize_schedule
from .preference_service import get_approved_teacher_preferences

DAY_NAME_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
}
PC_REQUIRED_LESSON_TYPES = {"lab"}
SUBGROUP_MODES = {"none", "auto", "forced"}
SEASON_ACADEMIC_PERIODS = {
    1: (1, 3, 5, 7),
    2: (2, 4, 6, 8),
}


def monday_for_week(target_year):
    today = date.today()
    safe_day = min(today.day, monthrange(target_year, today.month)[1])
    anchor = date(target_year, today.month, safe_day)
    return anchor - timedelta(days=anchor.weekday())


def _subgroup_label(index):
    label = ""
    value = index + 1
    while value:
        value, remainder = divmod(value - 1, 26)
        label = chr(65 + remainder) + label
    return label


def _int_at_least(value, minimum=1, default=None):
    if default is None:
        default = minimum
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return default


def _normalize_room_type(room):
    return str(room.get("type") or "").strip().lower()


def _room_matches_lesson_type(room, lesson_type, pc_required=False):
    if lesson_type == "lecture":
        return True
    if lesson_type == "practical" and pc_required:
        return _normalize_room_type(room) in {"lab", "practical"}
    return _normalize_room_type(room) == ("lab" if lesson_type == "lab" else "practical")


def _room_effective_capacity(room, lesson_type, pc_required=False):
    if not _room_matches_lesson_type(room, lesson_type, pc_required):
        return 0

    capacity = _int_at_least(room.get("capacity"), 0, 0)
    if pc_required or lesson_type in PC_REQUIRED_LESSON_TYPES:
        pc_count = _int_at_least(room.get("computer_count") or room.get("pcCount"), 0, 0)
        if pc_count <= 0:
            return 0
        return min(capacity, pc_count) if capacity > 0 else pc_count
    return capacity


def _max_room_capacity_for_lesson(rooms, lesson_type, pc_required=False):
    capacities = [_room_effective_capacity(room, lesson_type, pc_required) for room in rooms]
    return max(capacities, default=0)


def _resolve_subgroup_count(section, rooms):
    lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
    if lesson_type == "lecture":
        return 1

    mode = str(section.get("subgroup_mode") or "auto").strip().lower()
    mode = mode if mode in SUBGROUP_MODES else "auto"
    configured_count = _int_at_least(section.get("subgroup_count"), 1)

    if mode == "none":
        return 1
    if mode == "forced":
        return max(2, configured_count)

    student_count = _int_at_least(section.get("student_count"), 0, 0)
    pc_required = bool(section.get("requires_computers")) or lesson_type in PC_REQUIRED_LESSON_TYPES
    max_capacity = _max_room_capacity_for_lesson(rooms, lesson_type, pc_required)
    if student_count <= 0 or max_capacity <= 0 or student_count <= max_capacity:
        return 1
    return max(2, ceil(student_count / max_capacity))


def _subgroup_size(student_count, subgroup_count, index):
    if subgroup_count <= 1:
        return student_count
    base_size, remainder = divmod(max(0, student_count), subgroup_count)
    return max(1, base_size + (1 if index < remainder else 0))


def _room_type_required(lesson_type, pc_required=False):
    if lesson_type == "lecture":
        return "lecture"
    if lesson_type == "lab":
        return "lab"
    if lesson_type == "practical" and pc_required:
        return "any"
    return "practical"


def _build_optimizer_payload(sections, teachers, rooms, teacher_preferences):
    plan_items = []
    grouped_lectures = {}
    standalone_items = []

    for section in sections:
        base_group_id = section["group_name"] or str(section["group_id"])
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
        pc_required = bool(section.get("requires_computers")) or lesson_type in PC_REQUIRED_LESSON_TYPES
        base_item = {
            "courseId": section["course_id"],
            "courseName": section["course_name"],
            "teacherId": section["instructor_id"],
            "teacherName": section["instructor_name"],
            "groupIds": [base_group_id],
            "lessonsPerWeek": int(section.get("classes_count") or 0),
            "studentCount": int(section.get("student_count") or 0),
            "preferredBuildings": [],
            "preferredDays": [],
            "preferredHours": [],
            "preferredSlots": teacher_preferences.get(section["instructor_id"], []),
            "forbiddenSlots": [],
            "lessonType": lesson_type,
            "pcRequired": pc_required,
        }

        if lesson_type == "lecture":
            signature = (
                section["course_id"],
                section["instructor_id"],
                int(section.get("classes_count") or 0),
                section.get("group_language") or "",
                section.get("programme") or "",
            )
            grouped_lectures.setdefault(signature, []).append(section)
        else:
            subgroup_count = _resolve_subgroup_count(section, rooms)
            if subgroup_count <= 1:
                standalone_items.append(
                    {
                        **base_item,
                        "id": f"section_{section['id']}",
                        "lessonType": lesson_type,
                        "roomTypeRequired": _room_type_required(lesson_type, pc_required),
                        "streamId": f"{section['course_id']}-{section['group_id']}",
                        "subgroupIds": [],
                    }
                )
                continue

            student_count = _int_at_least(section.get("student_count"), 0, 0)
            for index in range(subgroup_count):
                subgroup = _subgroup_label(index)
                standalone_items.append(
                    {
                        **base_item,
                        "id": f"section_{section['id']}_{subgroup}",
                        "lessonType": lesson_type,
                        "roomTypeRequired": _room_type_required(lesson_type, pc_required),
                        "streamId": f"{section['course_id']}-{section['group_id']}",
                        "subgroupIds": [f"{base_group_id}-{subgroup}"],
                        "studentCount": _subgroup_size(student_count, subgroup_count, index),
                    }
                )

    for (course_id, instructor_id, classes_count, _group_language, _programme), lecture_sections in grouped_lectures.items():
        first_section = lecture_sections[0]
        plan_items.append(
            {
                "id": "stream_"
                + "_".join(str(section["id"]) for section in lecture_sections),
                "courseId": course_id,
                "courseName": first_section["course_name"],
                "teacherId": instructor_id,
                "teacherName": first_section["instructor_name"],
                "groupIds": [section["group_name"] or str(section["group_id"]) for section in lecture_sections],
                "lessonsPerWeek": classes_count,
                "studentCount": sum(int(section.get("student_count") or 0) for section in lecture_sections),
                "preferredBuildings": [],
                "preferredDays": [],
                "preferredHours": [],
                "preferredSlots": teacher_preferences.get(instructor_id, []),
                "forbiddenSlots": [],
                "lessonType": "lecture",
                "roomTypeRequired": "lecture",
                "subgroupIds": [],
                "streamId": f"lecture-{course_id}-{instructor_id}",
                "pcRequired": False,
            }
        )

    plan_items.extend(standalone_items)

    return {
        "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "hours": list(range(8, 18)),
        "preferSeparateSubgroupsByDay": False,
        "preferLowerFloors": True,
        "enforceLectureBeforeLab": True,
        "maxClassesPerDayForTeacher": 4,
        "maxClassesPerDayForAudience": 4,
        # Render free instances are memory-constrained, so keep the solver lean by default.
        "enableGapPenalties": False,
        "enableBuildingTransitionPenalties": False,
        "maxSolveTimeSeconds": 6,
        "numWorkers": 1,
        "teachers": [
            {
                "id": teacher["id"],
                "name": teacher["name"],
                "maxHoursPerWeek": teacher.get("weekly_hours_limit"),
                "availability": [],
                "teachingLanguages": teacher.get("teaching_languages", ""),
            }
            for teacher in teachers
        ],
        "rooms": [
            {
                "id": room["id"],
                "number": room["number"],
                "capacity": int(room.get("capacity") or 0),
                "type": room.get("type") or "",
                "building": room.get("building") or "",
                "floor": None,
                "pcCount": int(room.get("computer_count") or 0),
            }
            for room in rooms
        ],
        "planItems": plan_items,
    }


def _day_to_iso(selected_monday, day_name):
    day_index = DAY_NAME_TO_INDEX.get((day_name or "").strip().lower())
    if day_index is None:
        raise ApiError(400, "bad_request", f"Неизвестный день в оптимизаторе: {day_name}")
    return (selected_monday + timedelta(days=day_index)).isoformat()


def academic_periods_for_schedule_semester(semester):
    return SEASON_ACADEMIC_PERIODS.get(int(semester), (int(semester),))


def build_schedule(connection, semester, year, algorithm):
    academic_periods = academic_periods_for_schedule_semester(semester)
    placeholders = ", ".join("?" for _ in academic_periods)
    sections = query_all(
        connection,
        f"""
        SELECT
            s.id,
            s.course_id,
            s.course_name,
            s.group_id,
            s.group_name,
            s.classes_count,
            s.lesson_type,
            s.subgroup_mode,
            s.subgroup_count,
            s.requires_computers,
            COALESCE(s.teacher_id, c.instructor_id) AS instructor_id,
            COALESCE(NULLIF(s.teacher_name, ''), c.instructor_name, '') AS instructor_name,
            c.department,
            c.programme,
            c.semester,
            c.year,
            g.student_count,
            g.has_subgroups,
            g.language AS group_language,
            g.study_course
        FROM sections s
        JOIN courses c ON c.id = s.course_id
        JOIN groups g ON g.id = s.group_id
        WHERE c.semester IN ({placeholders})
          AND s.lesson_type IN ('lecture', 'practical', 'lab')
        ORDER BY g.student_count DESC, s.classes_count DESC, s.id
        """,
        tuple(academic_periods),
    )
    teachers = query_all(
        connection,
        """
        SELECT id, name, weekly_hours_limit, teaching_languages
        FROM teachers
        ORDER BY id
        """,
    )
    rooms = query_all(
        connection,
        """
        SELECT id, number, capacity, available, type, building, department, computer_count
        FROM rooms
        WHERE available = 1
        ORDER BY capacity, id
        """,
    )

    missing_parts = []
    if not sections:
        missing_parts.append(f"секции для академических периодов {', '.join(str(period) for period in academic_periods)}")
    if not teachers:
        missing_parts.append("преподаватели")
    if not rooms:
        missing_parts.append("доступные аудитории")

    if missing_parts:
        raise ApiError(
            400,
            "schedule_generation_requires_data",
            "Недостаточно данных для генерации расписания.",
            details={
                "semester": semester,
                "academicPeriods": list(academic_periods),
                "year": year,
                "missing": missing_parts,
            },
        )

    for section in sections:
        if not section.get("instructor_id"):
            raise ApiError(
                400,
                "bad_request",
                f"Для курса '{section['course_name']}' не найден преподаватель.",
            )

    teacher_language_map = {}
    for teacher in teachers:
        raw_languages = str(teacher.get("teaching_languages") or "ru,kk").split(",")
        teacher_language_map[teacher["id"]] = {
            language.strip().lower()
            for language in raw_languages
            if language.strip().lower() in {"ru", "kk"}
        } or {"ru", "kk"}

    for section in sections:
        if not section.get("study_course"):
            raise ApiError(
                400,
                "bad_request",
                f"Для группы '{section['group_name']}' не указан курс обучения.",
            )
        if int(section.get("study_course")) != int(section.get("year") or 0):
            raise ApiError(
                400,
                "bad_request",
                f"Дисциплина '{section['course_name']}' предназначена для {section['year']} курса, а группа '{section['group_name']}' указана как {section['study_course']} курс.",
            )
        group_language = str(section.get("group_language") or "ru").strip().lower()
        teacher_languages = teacher_language_map.get(section["instructor_id"], {"ru", "kk"})
        if group_language not in teacher_languages:
            raise ApiError(
                400,
                "bad_request",
                f"Преподаватель курса '{section['course_name']}' не поддерживает язык группы '{group_language}'.",
            )

    teacher_preference_rows = get_approved_teacher_preferences(connection)
    teacher_preferences = {}
    for row in teacher_preference_rows:
        teacher_preferences.setdefault(row["teacher_id"], []).append(
            {
                "day": row["preferred_day"].capitalize(),
                "hour": int(row["preferred_hour"]),
            }
        )

    payload = _build_optimizer_payload(sections, teachers, rooms, teacher_preferences)
    optimization_result = optimize_schedule(payload)
    generated_items = optimization_result.get("schedule") or []
    selected_monday = monday_for_week(year)

    section_lookup = {}
    for section in sections:
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
        if lesson_type == "lecture":
            continue
        section_lookup[f"section_{section['id']}"] = [
            {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": "",
            }
        ]
        for index in range(_resolve_subgroup_count(section, rooms)):
            subgroup = _subgroup_label(index)
            section_lookup[f"section_{section['id']}_{subgroup}"] = {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": subgroup,
            }

    lecture_groups = {}
    for section in sections:
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
        if lesson_type != "lecture":
            continue
        signature = (
            section["course_id"],
            section["instructor_id"],
            int(section.get("classes_count") or 0),
            section.get("group_language") or "",
            section.get("programme") or "",
        )
        lecture_groups.setdefault(signature, []).append(section)
    for signature, lecture_sections in lecture_groups.items():
        item_id = "stream_" + "_".join(str(section["id"]) for section in lecture_sections)
        section_lookup[item_id] = [
            {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": "",
            }
            for section in lecture_sections
        ]

    rows = []
    for item in generated_items:
        section_entries = section_lookup.get(item["itemId"])
        if section_entries is None:
            raise ApiError(
                400,
                "bad_request",
                f"Оптимизатор вернул неизвестную секцию: {item['itemId']}",
            )
        if isinstance(section_entries, dict):
            section_entries = [section_entries]
        for section_meta in section_entries:
            rows.append(
                (
                    section_meta["section_id"],
                    item.get("courseId"),
                    item.get("courseName"),
                    item.get("teacherId"),
                    item.get("teacherName"),
                    item.get("roomId"),
                    item.get("roomNumber"),
                    section_meta["group_id"],
                    section_meta["group_name"],
                    section_meta["subgroup"],
                    _day_to_iso(selected_monday, item.get("day")),
                    int(item.get("hour")),
                    semester,
                    year,
                    algorithm or "optimizer",
                )
            )

    db_execute(
        connection,
        """
        DELETE FROM schedules
        WHERE semester = ? AND year = ?
        """,
        (semester, year),
    )
    db_executemany(
        connection,
        """
        INSERT INTO schedules (
            section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()

    return query_all(
        connection,
        """
        SELECT
            id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
        FROM schedules
        WHERE semester = ? AND year = ?
        ORDER BY day, start_hour, id
        """,
        (semester, year),
    )
