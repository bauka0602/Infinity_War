from calendar import monthrange
from copy import deepcopy
from datetime import date, datetime, timedelta

from .db import db_execute, db_executemany, query_all
from .education_programmes import get_home_room_programmes
from .errors import ApiError
from .optimizer import optimize_schedule
from .preference_service import get_approved_teacher_preferences
from .programme_utils import normalize_programme_text
from .room_availability import get_room_blocked_slots, recompute_room_availability
from .time_slots import SCHEDULE_HOURS

DAY_NAME_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
}
PC_REQUIRED_LESSON_TYPES = {"lab"}
MIN_COMPUTER_COUNT = 10
PHYSICAL_EDUCATION_ROOM_NUMBER = "орленок"
SUBGROUP_MODES = {"none", "auto", "forced"}
MAX_GENERATED_SUBGROUPS = 2
USE_GREEDY_BATCH_SCHEDULER = True
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
    normalized_room_type = _normalize_room_type(room)
    if lesson_type == "lecture":
        return normalized_room_type == "lecture"
    if lesson_type == "practical":
        return normalized_room_type in {"practical", "lecture"}
    if lesson_type == "lab":
        return normalized_room_type == "practical"
    return normalized_room_type == "practical"


def _room_effective_capacity(room, lesson_type, pc_required=False):
    if not _room_matches_lesson_type(room, lesson_type, pc_required):
        return 0

    capacity = _int_at_least(room.get("capacity"), 0, 0)
    if pc_required or lesson_type in PC_REQUIRED_LESSON_TYPES:
        pc_count = _int_at_least(room.get("computer_count") or room.get("pcCount"), 0, 0)
        if pc_count < MIN_COMPUTER_COUNT:
            return 0
        return capacity
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
        return min(MAX_GENERATED_SUBGROUPS, max(2, configured_count))

    student_count = _int_at_least(section.get("student_count"), 0, 0)
    pc_required = bool(section.get("requires_computers")) or lesson_type in PC_REQUIRED_LESSON_TYPES
    if _is_physical_education(section):
        rooms = [
            room
            for room in rooms
            if PHYSICAL_EDUCATION_ROOM_NUMBER in str(room.get("number") or "").strip().lower()
        ]
    max_capacity = _max_room_capacity_for_lesson(rooms, lesson_type, pc_required)
    if student_count <= 0 or max_capacity <= 0 or student_count <= max_capacity:
        return 1
    return MAX_GENERATED_SUBGROUPS


def _subgroup_size(student_count, subgroup_count, index):
    if subgroup_count <= 1:
        return student_count
    base_size, remainder = divmod(max(0, student_count), subgroup_count)
    return max(1, base_size + (1 if index < remainder else 0))


def _room_type_required(lesson_type, pc_required=False):
    if lesson_type == "lecture":
        return "lecture"
    return "practical"


def _is_physical_education(section):
    text = " ".join(
        str(section.get(field) or "").lower()
        for field in ("course_name", "course_code")
    )
    return (
        "физическая культура" in text
        or "дене шынықтыру" in text
        or "fk " in f"{text} "
    )


def _build_optimizer_payload(sections, teachers, rooms, teacher_preferences):
    plan_items = []
    grouped_lectures = {}
    standalone_items = []

    for section in sections:
        preferred_room_programmes = get_home_room_programmes(
            section.get("group_programme"),
            section.get("specialty_code"),
        )
        base_group_id = section["group_name"] or str(section["group_id"])
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
        pc_required = bool(section.get("requires_computers")) or lesson_type in PC_REQUIRED_LESSON_TYPES
        base_item = {
            "courseId": section["course_id"],
            "courseName": section["course_name"],
            "teacherId": section["instructor_id"],
            "teacherName": section["instructor_name"],
            "programme": section.get("programme") or "",
            "preferredRoomProgrammes": preferred_room_programmes,
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
        if _is_physical_education(section):
            base_item["allowedRoomNumbers"] = [PHYSICAL_EDUCATION_ROOM_NUMBER]
            base_item["roomTypeRequired"] = "practical"

        if lesson_type == "lecture":
            signature = (
                section["course_id"],
                section["instructor_id"],
                int(section.get("classes_count") or 0),
                section.get("group_language") or "",
                normalize_programme_text(section.get("programme") or ""),
                tuple(sorted(normalize_programme_text(value) for value in preferred_room_programmes)),
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

    for (
        course_id,
        instructor_id,
        classes_count,
        _group_language,
        _programme,
        _preferred_room_programmes,
    ), lecture_sections in grouped_lectures.items():
        first_section = lecture_sections[0]
        lecture_preferred_room_programmes = sorted(
            {
                programme
                for section in lecture_sections
                for programme in get_home_room_programmes(
                    section.get("group_programme"),
                    section.get("specialty_code"),
                )
            }
        )
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
                "preferredRoomProgrammes": lecture_preferred_room_programmes,
            }
        )

    plan_items.extend(standalone_items)

    return {
        "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "hours": SCHEDULE_HOURS,
        "preferSeparateSubgroupsByDay": False,
        "preferLowerFloors": True,
        "enforceLectureBeforeLab": True,
        "maxClassesPerDayForTeacher": 6,
        "maxClassesPerDayForAudience": 6,
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
                "programme": room.get("programme") or "",
                "building": room.get("building") or "",
                "floor": None,
                "pcCount": int(room.get("computer_count") or 0),
                "unavailableSlots": room.get("unavailable_slots") or [],
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


def _room_block_day_for_optimizer(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw).strftime("%A")
    except ValueError:
        return raw


def academic_periods_for_schedule_semester(semester):
    return SEASON_ACADEMIC_PERIODS.get(int(semester), (int(semester),))


def _build_section_lookup(sections, rooms):
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
            normalize_programme_text(section.get("programme") or ""),
            tuple(
                sorted(
                    normalize_programme_text(value)
                    for value in get_home_room_programmes(
                        section.get("group_programme"),
                        section.get("specialty_code"),
                    )
                )
            ),
        )
        lecture_groups.setdefault(signature, []).append(section)
    for _signature, lecture_sections in lecture_groups.items():
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
    return section_lookup


def _rows_from_generated_items(generated_items, section_lookup, selected_monday, semester, year, algorithm):
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
                    item.get("roomProgramme") or "",
                    1 if item.get("roomProgrammeFallbackUsed") else 0,
                    "",
                    "",
                )
            )
    return rows


def _merge_slot_lists(left, right):
    seen = {(item.get("day"), int(item.get("hour"))) for item in left}
    merged = list(left)
    for item in right:
        key = (item.get("day"), int(item.get("hour")))
        if key not in seen:
            seen.add(key)
            merged.append({"day": item.get("day"), "hour": int(item.get("hour"))})
    return merged


def _apply_batch_occupancy(payload, occupied):
    room_slots = occupied["rooms"]
    teacher_slots = occupied["teachers"]
    group_slots = occupied["groups"]

    for room in payload["rooms"]:
        blocked = [
            {"day": day, "hour": hour}
            for day, hour in sorted(room_slots.get(room["id"], set()))
        ]
        if blocked:
            room["unavailableSlots"] = _merge_slot_lists(room.get("unavailableSlots") or [], blocked)

    for item in payload["planItems"]:
        forbidden = [
            {"day": day, "hour": hour}
            for day, hour in sorted(teacher_slots.get(item.get("teacherId"), set()))
        ]
        for group_id in item.get("groupIds") or []:
            forbidden.extend(
                {"day": day, "hour": hour}
                for day, hour in sorted(group_slots.get(str(group_id), set()))
            )
        if forbidden:
            item["forbiddenSlots"] = _merge_slot_lists(item.get("forbiddenSlots") or [], forbidden)
    return payload


def _record_batch_occupancy(occupied, generated_items):
    for item in generated_items:
        slot = (item.get("day"), int(item.get("hour")))
        occupied["rooms"].setdefault(item.get("roomId"), set()).add(slot)
        occupied["teachers"].setdefault(item.get("teacherId"), set()).add(slot)
        for group_id in item.get("groups") or []:
            occupied["groups"].setdefault(str(group_id), set()).add(slot)


def _slot_key_from_raw(raw):
    return (str(raw.get("day")), int(raw.get("hour")))


def _greedy_room_candidates(item, rooms, day, hour, room_unavailable=None):
    lesson_type = (item.get("lessonType") or "lecture").strip().lower()
    pc_required = bool(item.get("pcRequired")) or lesson_type in PC_REQUIRED_LESSON_TYPES
    allowed_numbers = {
        str(value).strip().lower()
        for value in (item.get("allowedRoomNumbers") or [])
        if str(value).strip()
    }
    candidates = []
    for room in rooms:
        room_number = str(room.get("number") or "").strip().lower()
        if allowed_numbers and not any(value == room_number or value in room_number for value in allowed_numbers):
            continue
        if room_unavailable is None:
            unavailable = {_slot_key_from_raw(slot) for slot in room.get("unavailableSlots") or []}
        else:
            unavailable = room_unavailable.get(room.get("id"), set())
        if (day, hour) in unavailable:
            continue
        room_for_match = {
            "type": room.get("type") or "",
            "capacity": int(room.get("capacity") or 0),
            "computer_count": int(room.get("pcCount") or room.get("computer_count") or 0),
        }
        if not _room_matches_lesson_type(room_for_match, lesson_type, pc_required=pc_required):
            continue
        if int(room.get("capacity") or 0) < int(item.get("studentCount") or 0):
            continue
        if pc_required and int(room.get("pcCount") or 0) < MIN_COMPUTER_COUNT:
            continue
        type_score = 1 if _normalize_room_type(room_for_match) == _room_type_required(lesson_type, pc_required) else 0
        candidates.append((type_score, int(room.get("capacity") or 0), str(room.get("number") or ""), room))
    candidates.sort(key=lambda entry: (-entry[0], entry[1], entry[2]))
    return [entry[3] for entry in candidates]


def _greedy_optimize_batch(payload):
    days = payload.get("days") or ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    hours = payload.get("hours") or SCHEDULE_HOURS
    slots = [(str(day), int(hour)) for day in days for hour in hours]
    max_teacher_day = int(payload.get("maxClassesPerDayForTeacher") or 4)
    max_audience_day = int(payload.get("maxClassesPerDayForAudience") or 4)

    room_unavailable = {
        room.get("id"): {_slot_key_from_raw(slot) for slot in room.get("unavailableSlots") or []}
        for room in payload.get("rooms") or []
    }
    room_busy = set()
    teacher_busy = set()
    group_busy = set()
    teacher_day_count = {}
    group_day_count = {}
    generated = []

    items = sorted(
        payload.get("planItems") or [],
        key=lambda item: (
            0 if (item.get("lessonType") or "").strip().lower() == "lecture" else 1,
            -int(item.get("lessonsPerWeek") or 0),
            str(item.get("courseName") or ""),
        ),
    )

    for item in items:
        forbidden = {_slot_key_from_raw(slot) for slot in item.get("forbiddenSlots") or []}
        lessons_left = int(item.get("lessonsPerWeek") or 0)
        for _lesson_index in range(lessons_left):
            placed = None
            for day, hour in slots:
                if (day, hour) in forbidden:
                    continue
                teacher_key = (item.get("teacherId"), day, hour)
                if teacher_key in teacher_busy:
                    continue
                if teacher_day_count.get((item.get("teacherId"), day), 0) >= max_teacher_day:
                    continue
                group_ids = [str(group_id) for group_id in item.get("groupIds") or []]
                if any((group_id, day, hour) in group_busy for group_id in group_ids):
                    continue
                if any(group_day_count.get((group_id, day), 0) >= max_audience_day for group_id in group_ids):
                    continue
                for room in _greedy_room_candidates(
                    item,
                    payload.get("rooms") or [],
                    day,
                    hour,
                    room_unavailable=room_unavailable,
                ):
                    room_key = (room.get("id"), day, hour)
                    if (day, hour) in room_unavailable.get(room.get("id"), set()):
                        continue
                    if room_key in room_busy:
                        continue
                    placed = (day, hour, room)
                    break
                if placed:
                    break
            if not placed:
                raise ApiError(
                    400,
                    "optimizer_no_solution",
                    "Не удалось найти допустимое расписание для заданных ограничений.",
                    details={
                        "itemId": item.get("id"),
                        "courseName": item.get("courseName"),
                        "lessonType": item.get("lessonType"),
                    },
                )
            day, hour, room = placed
            room_busy.add((room.get("id"), day, hour))
            teacher_busy.add((item.get("teacherId"), day, hour))
            teacher_day_count[(item.get("teacherId"), day)] = teacher_day_count.get((item.get("teacherId"), day), 0) + 1
            for group_id in [str(group_id) for group_id in item.get("groupIds") or []]:
                group_busy.add((group_id, day, hour))
                group_day_count[(group_id, day)] = group_day_count.get((group_id, day), 0) + 1
            generated.append(
                {
                    "itemId": item.get("id"),
                    "courseId": item.get("courseId"),
                    "courseName": item.get("courseName"),
                    "teacherId": item.get("teacherId"),
                    "teacherName": item.get("teacherName"),
                    "roomId": room.get("id"),
                    "roomNumber": room.get("number"),
                    "roomProgramme": room.get("programme") or "",
                    "roomProgrammeFallbackUsed": False,
                    "groups": [str(group_id) for group_id in item.get("groupIds") or []],
                    "subgroups": item.get("subgroupIds") or [],
                    "streamId": item.get("streamId"),
                    "day": day,
                    "hour": hour,
                }
            )
    return {"status": "GREEDY", "schedule": generated, "diagnostics": {"fallback": "greedy"}}


def _schedule_batch_key(section):
    return (
        int(section.get("study_course") or 0),
        str(section.get("group_language") or "").strip().lower(),
        normalize_programme_text(section.get("group_programme") or section.get("programme") or ""),
        normalize_programme_text(section.get("specialty_code") or ""),
    )


def _generate_schedule_rows_by_batches(sections, teachers, rooms, teacher_preferences, semester, year, algorithm):
    selected_monday = monday_for_week(year)
    rows = []
    occupied = {"rooms": {}, "teachers": {}, "groups": {}}
    batch_keys = sorted(
        {_schedule_batch_key(section) for section in sections},
        key=lambda key: (-key[0], key[2], key[1], key[3]),
    )
    for batch_key in batch_keys:
        study_course = batch_key[0]
        batch_sections = [
            section
            for section in sections
            if _schedule_batch_key(section) == batch_key
        ]
        if not batch_sections:
            continue
        payload = _build_optimizer_payload(batch_sections, teachers, deepcopy(rooms), teacher_preferences)
        payload = _apply_batch_occupancy(payload, occupied)
        if USE_GREEDY_BATCH_SCHEDULER:
            optimization_result = _greedy_optimize_batch(payload)
        else:
            try:
                optimization_result = optimize_schedule(payload)
            except ApiError as exc:
                if exc.code == "optimizer_no_solution":
                    try:
                        optimization_result = _greedy_optimize_batch(payload)
                    except ApiError as fallback_exc:
                        details = getattr(fallback_exc, "details", None) or {}
                        if isinstance(details, dict):
                            details = {
                                **details,
                                "studyCourse": study_course,
                                "batchKey": {
                                    "language": batch_key[1],
                                    "programme": batch_key[2],
                                    "specialtyCode": batch_key[3],
                                },
                                "batchSections": len(batch_sections),
                                "batchPlanItems": len(payload.get("planItems") or []),
                            }
                        raise ApiError(
                            fallback_exc.status,
                            fallback_exc.code,
                            f"{fallback_exc.message} Пакет: {study_course} курс, {batch_key[2] or 'без направления'}, {batch_key[1] or 'без языка'}.",
                            details=details,
                        ) from fallback_exc
                else:
                    details = getattr(exc, "details", None) or {}
                    if isinstance(details, dict):
                        details = {
                            **details,
                            "studyCourse": study_course,
                            "batchKey": {
                                "language": batch_key[1],
                                "programme": batch_key[2],
                                "specialtyCode": batch_key[3],
                            },
                            "batchSections": len(batch_sections),
                            "batchPlanItems": len(payload.get("planItems") or []),
                        }
                    raise ApiError(
                        exc.status,
                        exc.code,
                        f"{exc.message} Пакет: {study_course} курс, {batch_key[2] or 'без направления'}, {batch_key[1] or 'без языка'}.",
                        details=details,
                    ) from exc
        generated_items = optimization_result.get("schedule") or []
        section_lookup = _build_section_lookup(batch_sections, rooms)
        rows.extend(_rows_from_generated_items(generated_items, section_lookup, selected_monday, semester, year, algorithm))
        _record_batch_occupancy(occupied, generated_items)
    return rows


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
            g.study_course,
            g.programme AS group_programme,
            g.specialty_code
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
        SELECT id, number, capacity, available, type, '' AS building, programme, computer_count
        FROM rooms
        WHERE coalesce(available, 1) = 1
        ORDER BY capacity, id
        """,
    )
    room_blocked_slots = get_room_blocked_slots(connection, semester, year)
    for room in rooms:
        room["unavailable_slots"] = [
            {"day": _room_block_day_for_optimizer(day), "hour": hour}
            for day, hour in sorted(room_blocked_slots.get(room["id"], set()))
            if _room_block_day_for_optimizer(day)
        ]

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

    rows = _generate_schedule_rows_by_batches(
        sections,
        teachers,
        rooms,
        teacher_preferences,
        semester,
        year,
        algorithm,
    )

    generated_group_ids = sorted({int(section["group_id"]) for section in sections if section.get("group_id") is not None})

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
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
            room_programme, room_programme_mismatch, relocated_from_room_number, relocation_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    if generated_group_ids:
        reset_placeholders = ", ".join("?" for _ in generated_group_ids)
        db_execute(
            connection,
            f"""
            UPDATE groups
            SET has_subgroups = 0
            WHERE id IN ({reset_placeholders})
            """,
            tuple(generated_group_ids),
        )

    generated_subgroup_group_ids = sorted(
        {
            int(row[7])
            for row in rows
            if str(row[9] or "").strip()
        }
    )
    if generated_subgroup_group_ids:
        placeholders = ", ".join("?" for _ in generated_subgroup_group_ids)
        db_execute(
            connection,
            f"""
            UPDATE groups
            SET has_subgroups = 1
            WHERE id IN ({placeholders})
            """,
            tuple(generated_subgroup_group_ids),
        )

    recompute_room_availability(connection)
    connection.commit()

    return query_all(
        connection,
        """
        SELECT
            id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm,
            room_programme, room_programme_mismatch, relocated_from_room_number, relocation_reason
        FROM schedules
        WHERE semester = ? AND year = ?
        ORDER BY day, start_hour, id
        """,
        (semester, year),
    )
