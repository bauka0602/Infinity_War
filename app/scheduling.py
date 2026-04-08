from datetime import date, timedelta

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


def monday_for_week(target_year):
    today = date.today()
    anchor = date(target_year, today.month, today.day)
    return anchor - timedelta(days=anchor.weekday())


def _build_optimizer_payload(sections, teachers, rooms, teacher_preferences):
    plan_items = []
    grouped_lectures = {}
    standalone_items = []

    for section in sections:
        base_group_id = section["group_name"] or str(section["group_id"])
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
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
            "pcRequired": bool(section.get("requires_computers")),
        }

        if lesson_type == "lecture":
            signature = (
                section["course_id"],
                section["instructor_id"],
                int(section.get("classes_count") or 0),
            )
            grouped_lectures.setdefault(signature, []).append(section)
        elif section.get("has_subgroups"):
            subgroup_size = max(1, int((section.get("student_count") or 0) / 2))
            for subgroup in ("A", "B"):
                standalone_items.append(
                    {
                        **base_item,
                        "id": f"section_{section['id']}_{subgroup}",
                        "lessonType": lesson_type,
                        "roomTypeRequired": "lab" if lesson_type == "lab" else "practical",
                        "streamId": f"{section['course_id']}-{section['group_id']}",
                        "subgroupIds": [f"{base_group_id}-{subgroup}"],
                        "studentCount": subgroup_size,
                    }
                )
        else:
            standalone_items.append(
                {
                    **base_item,
                    "id": f"section_{section['id']}",
                    "lessonType": lesson_type,
                    "roomTypeRequired": "lab" if lesson_type == "lab" else "practical",
                    "streamId": f"{section['course_id']}-{section['group_id']}",
                    "subgroupIds": [],
                }
            )

    for (course_id, instructor_id, classes_count), lecture_sections in grouped_lectures.items():
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
                "pcRequired": bool(first_section.get("requires_computers")),
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


def build_schedule(connection, semester, year, algorithm):
    sections = query_all(
        connection,
        """
        SELECT
            s.id,
            s.course_id,
            s.course_name,
            s.group_id,
            s.group_name,
            s.classes_count,
            s.lesson_type,
            c.instructor_id,
            c.instructor_name,
            c.department,
            c.semester,
            c.year,
            c.requires_computers,
            g.student_count,
            g.has_subgroups,
            g.language AS group_language
        FROM sections s
        JOIN courses c ON c.id = s.course_id
        JOIN groups g ON g.id = s.group_id
        WHERE c.semester = ? AND c.year = ?
        ORDER BY g.student_count DESC, s.classes_count DESC, s.id
        """,
        (semester, year),
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
        missing_parts.append(f"секции для {semester} семестра {year} года")
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
        if section.get("has_subgroups"):
            section_lookup[f"section_{section['id']}_A"] = {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": "A",
            }
            section_lookup[f"section_{section['id']}_B"] = {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": "B",
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
