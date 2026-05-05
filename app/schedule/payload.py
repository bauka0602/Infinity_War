from ..programmes.education import get_home_room_programmes
from ..programmes.utils import normalize_programme_text
from .config import CP_SAT_SOLVE_SECONDS
from .time_slots import SCHEDULE_HOURS

PC_REQUIRED_LESSON_TYPES = {"lab"}
MIN_COMPUTER_COUNT = 10
PHYSICAL_EDUCATION_ROOM_NUMBER = "орленок"
SUBGROUP_MODES = {"none", "auto", "forced"}
MAX_GENERATED_SUBGROUPS = 2


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


def _is_first_year_section(section):
    try:
        return int(section.get("study_course") or 0) == 1
    except (TypeError, ValueError):
        return False


def _lecture_stream_id(lecture_sections):
    return "stream_" + "_".join(str(section["id"]) for section in lecture_sections)


def _chunk_sections_by_capacity(sections, max_capacity):
    if max_capacity <= 0:
        return [sections]

    chunks = []
    current = []
    current_count = 0
    ordered_sections = sorted(
        sections,
        key=lambda section: _int_at_least(section.get("student_count"), 0, 0),
        reverse=True,
    )
    for section in ordered_sections:
        student_count = _int_at_least(section.get("student_count"), 0, 0)
        if current and current_count + student_count > max_capacity:
            chunks.append(current)
            current = []
            current_count = 0
        current.append(section)
        current_count += student_count
    if current:
        chunks.append(current)
    return chunks


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
            base_item["preferLastLesson"] = True
        else:
            base_item["forbiddenRoomNumbers"] = [PHYSICAL_EDUCATION_ROOM_NUMBER]

        if lesson_type == "lecture" and _is_first_year_section(section):
            signature = (
                section["course_id"],
                section["instructor_id"],
                int(section.get("classes_count") or 0),
                section.get("group_language") or "",
                normalize_programme_text(section.get("programme") or ""),
                tuple(sorted(normalize_programme_text(value) for value in preferred_room_programmes)),
            )
            grouped_lectures.setdefault(signature, []).append(section)
        elif lesson_type == "lecture":
            standalone_items.append(
                {
                    **base_item,
                    "id": f"section_{section['id']}",
                    "lessonType": lesson_type,
                    "roomTypeRequired": "lecture",
                    "streamId": f"{section['course_id']}-{section['group_id']}",
                    "subgroupIds": [],
                }
            )
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
        max_lecture_capacity = _max_room_capacity_for_lesson(rooms, "lecture")
        for lecture_chunk in _chunk_sections_by_capacity(lecture_sections, max_lecture_capacity):
            first_section = lecture_chunk[0]
            lecture_preferred_room_programmes = sorted(
                {
                    programme
                    for section in lecture_chunk
                    for programme in get_home_room_programmes(
                        section.get("group_programme"),
                        section.get("specialty_code"),
                    )
                }
            )
            stream_id = _lecture_stream_id(lecture_chunk)
            plan_items.append(
                {
                    "id": stream_id,
                    "courseId": course_id,
                    "courseName": first_section["course_name"],
                    "teacherId": instructor_id,
                    "teacherName": first_section["instructor_name"],
                    "groupIds": [section["group_name"] or str(section["group_id"]) for section in lecture_chunk],
                    "lessonsPerWeek": classes_count,
                    "studentCount": sum(int(section.get("student_count") or 0) for section in lecture_chunk),
                    "preferredBuildings": [],
                    "preferredDays": [],
                    "preferredHours": [],
                    "preferredSlots": teacher_preferences.get(instructor_id, []),
                    "forbiddenSlots": [],
                    "lessonType": "lecture",
                    "roomTypeRequired": "lecture",
                    "subgroupIds": [],
                    "streamId": f"lecture-{course_id}-{instructor_id}-{stream_id}",
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
        # CP-SAT can need several minutes on dense real schedules.
        "enableGapPenalties": False,
        "enableBuildingTransitionPenalties": False,
        "maxSolveTimeSeconds": CP_SAT_SOLVE_SECONDS,
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
