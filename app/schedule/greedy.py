from ..core.errors import ApiError
from .time_slots import SCHEDULE_HOURS

PC_REQUIRED_LESSON_TYPES = {"lab"}
MIN_COMPUTER_COUNT = 10


def _normalize_room_type(room):
    return str(room.get("type") or "").strip().lower()


def _room_type_required(lesson_type, pc_required=False):
    if lesson_type == "lecture":
        return "lecture"
    return "practical"


def _room_matches_lesson_type(room, lesson_type, pc_required=False):
    normalized_room_type = _normalize_room_type(room)
    if lesson_type == "lecture":
        return normalized_room_type == "lecture"
    if lesson_type == "practical":
        return normalized_room_type in {"practical", "lecture"}
    if lesson_type == "lab":
        return normalized_room_type == "practical"
    return normalized_room_type == "practical"


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
    forbidden_numbers = {
        str(value).strip().lower()
        for value in (item.get("forbiddenRoomNumbers") or [])
        if str(value).strip()
    }
    candidates = []
    for room in rooms:
        room_number = str(room.get("number") or "").strip().lower()
        if allowed_numbers and not any(value == room_number or value in room_number for value in allowed_numbers):
            continue
        if forbidden_numbers and any(value == room_number or value in room_number for value in forbidden_numbers):
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


def _rebuild_usage(generated):
    room_busy = set()
    teacher_busy = set()
    group_busy = set()
    teacher_day_count = {}
    group_day_count = {}
    day_load_count = {}
    for entry in generated:
        day = entry["day"]
        hour = int(entry["hour"])
        room_busy.add((entry.get("roomId"), day, hour))
        teacher_busy.add((entry.get("teacherId"), day, hour))
        teacher_day_count[(entry.get("teacherId"), day)] = (
            teacher_day_count.get((entry.get("teacherId"), day), 0) + 1
        )
        day_load_count[day] = day_load_count.get(day, 0) + 1
        for group_id in entry.get("groups") or []:
            group_busy.add((group_id, day, hour))
            group_day_count[(group_id, day)] = group_day_count.get((group_id, day), 0) + 1
    return room_busy, teacher_busy, group_busy, teacher_day_count, group_day_count, day_load_count


def _slot_index(slot_indexes, day, hour):
    return slot_indexes.get((day, int(hour)), 10**9)


def _precedence_is_valid(generated, item_by_id, moving_entry, day, hour, slot_indexes):
    moving_item = item_by_id.get(moving_entry.get("itemId"), {})
    moving_lesson_type = (moving_item.get("lessonType") or "").strip().lower()
    moving_course_id = moving_entry.get("courseId")
    proposed_index = _slot_index(slot_indexes, day, hour)
    for entry in generated:
        if entry is moving_entry or entry.get("courseId") != moving_course_id:
            continue
        if not set(entry.get("groups") or []).intersection(moving_entry.get("groups") or []):
            continue
        other_item = item_by_id.get(entry.get("itemId"), {})
        other_lesson_type = (other_item.get("lessonType") or "").strip().lower()
        other_index = _slot_index(slot_indexes, entry.get("day"), entry.get("hour"))
        if moving_lesson_type == "lecture" and other_lesson_type in {"practical", "lab"}:
            if proposed_index >= other_index:
                return False
        if moving_lesson_type in {"practical", "lab"} and other_lesson_type == "lecture":
            if other_index >= proposed_index:
                return False
    return True


def _rebalance_generated_schedule(
    generated,
    payload,
    item_by_id,
    days,
    hours,
    slot_indexes,
    room_unavailable,
    max_teacher_day,
    max_audience_day,
):
    if len(days) < 2 or not generated:
        return generated

    rooms = payload.get("rooms") or []
    for _attempt in range(len(generated) * 2):
        (
            room_busy,
            teacher_busy,
            group_busy,
            teacher_day_count,
            group_day_count,
            day_load_count,
        ) = _rebuild_usage(generated)
        heavy_day = max(days, key=lambda day: day_load_count.get(day, 0))
        lightest_day = min(days, key=lambda day: day_load_count.get(day, 0))
        if day_load_count.get(heavy_day, 0) - day_load_count.get(lightest_day, 0) <= 8:
            break

        moved = False
        candidates = [
            entry
            for entry in generated
            if entry.get("day") == heavy_day
            and not bool(item_by_id.get(entry.get("itemId"), {}).get("preferLastLesson"))
        ]
        candidates.sort(
            key=lambda entry: (
                _slot_index(slot_indexes, entry.get("day"), entry.get("hour")),
                str(entry.get("courseName") or ""),
            ),
            reverse=True,
        )

        for entry in candidates:
            item = item_by_id.get(entry.get("itemId"))
            if not item:
                continue

            old_day = entry["day"]
            old_hour = int(entry["hour"])
            old_room_id = entry.get("roomId")
            teacher_id = entry.get("teacherId")
            group_ids = [str(group_id) for group_id in entry.get("groups") or []]

            room_busy.discard((old_room_id, old_day, old_hour))
            teacher_busy.discard((teacher_id, old_day, old_hour))
            teacher_day_count[(teacher_id, old_day)] = max(
                0, teacher_day_count.get((teacher_id, old_day), 0) - 1
            )
            day_load_count[old_day] = max(0, day_load_count.get(old_day, 0) - 1)
            for group_id in group_ids:
                group_busy.discard((group_id, old_day, old_hour))
                group_day_count[(group_id, old_day)] = max(
                    0, group_day_count.get((group_id, old_day), 0) - 1
                )

            target_days = sorted(
                [day for day in days if day != heavy_day],
                key=lambda day: (day_load_count.get(day, 0), day),
            )
            target_slots = [(target_day, int(hour)) for target_day in target_days for hour in hours]
            target_slots.sort(
                key=lambda slot: (
                    max(
                        (
                            group_day_count.get((group_id, slot[0]), 0)
                            for group_id in group_ids
                        ),
                        default=0,
                    ),
                    teacher_day_count.get((teacher_id, slot[0]), 0),
                    _slot_index(slot_indexes, slot[0], slot[1]),
                )
            )

            for day, hour in target_slots:
                if (day, hour) in {
                    _slot_key_from_raw(slot) for slot in item.get("forbiddenSlots") or []
                }:
                    continue
                if (teacher_id, day, hour) in teacher_busy:
                    continue
                if teacher_day_count.get((teacher_id, day), 0) >= max_teacher_day:
                    continue
                if any((group_id, day, hour) in group_busy for group_id in group_ids):
                    continue
                if any(
                    group_day_count.get((group_id, day), 0) >= max_audience_day
                    for group_id in group_ids
                ):
                    continue
                if not _precedence_is_valid(generated, item_by_id, entry, day, hour, slot_indexes):
                    continue
                for room in _greedy_room_candidates(
                    item,
                    rooms,
                    day,
                    hour,
                    room_unavailable=room_unavailable,
                ):
                    if (room.get("id"), day, hour) in room_busy:
                        continue
                    entry["day"] = day
                    entry["hour"] = hour
                    entry["roomId"] = room.get("id")
                    entry["roomNumber"] = room.get("number")
                    entry["roomProgramme"] = room.get("programme") or ""
                    moved = True
                    break
                if moved:
                    break

            if moved:
                break

        if not moved:
            break

    return generated


def optimize_greedy_schedule(payload):
    days = payload.get("days") or ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    hours = payload.get("hours") or SCHEDULE_HOURS
    slots = [(str(day), int(hour)) for day in days for hour in hours]
    slot_indexes = {slot: index for index, slot in enumerate(slots)}
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
    group_day_max_hour = {}
    day_load_count = {str(day): 0 for day in days}
    lecture_required_keys = set()
    lecture_slots_by_course_group = {}
    generated = []

    items = sorted(
        payload.get("planItems") or [],
        key=lambda item: (
            2
            if item.get("preferLastLesson")
            else 0
            if (item.get("lessonType") or "").strip().lower() == "lecture"
            else 1,
            -int(item.get("lessonsPerWeek") or 0),
            str(item.get("courseName") or ""),
        ),
    )
    for item in items:
        if (item.get("lessonType") or "").strip().lower() != "lecture":
            continue
        course_id = item.get("courseId")
        for group_id in [str(group_id) for group_id in item.get("groupIds") or []]:
            lecture_required_keys.add((course_id, group_id))

    for item in items:
        forbidden = {_slot_key_from_raw(slot) for slot in item.get("forbiddenSlots") or []}
        lesson_type = (item.get("lessonType") or "lecture").strip().lower()
        course_id = item.get("courseId")
        prefer_last_lesson = bool(item.get("preferLastLesson"))
        lessons_left = int(item.get("lessonsPerWeek") or 0)
        for _lesson_index in range(lessons_left):
            placement_candidates = []
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
                slot_index = slot_indexes[(day, hour)]
                if lesson_type in {"practical", "lab"}:
                    lecture_is_later_or_missing = False
                    for group_id in group_ids:
                        lecture_key = (course_id, group_id)
                        if lecture_key not in lecture_required_keys:
                            continue
                        lecture_slot_index = lecture_slots_by_course_group.get(lecture_key)
                        if lecture_slot_index is None or lecture_slot_index >= slot_index:
                            lecture_is_later_or_missing = True
                            break
                    if lecture_is_later_or_missing:
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
                    group_day_values = [
                        group_day_count.get((group_id, day), 0)
                        for group_id in group_ids
                    ]
                    max_group_day_load = max(group_day_values, default=0)
                    group_day_load_sum = sum(group_day_values)
                    if lesson_type == "lecture":
                        placement_rank = (
                            slot_index,
                            max_group_day_load,
                            teacher_day_count.get((item.get("teacherId"), day), 0),
                            day_load_count.get(day, 0),
                            group_day_load_sum,
                        )
                    elif prefer_last_lesson:
                        existing_group_day_max_hour = max(
                            (
                                group_day_max_hour.get((group_id, day), -1)
                                for group_id in group_ids
                            ),
                            default=-1,
                        )
                        last_lesson_penalty = 0 if hour >= existing_group_day_max_hour else 1
                        placement_rank = (
                            last_lesson_penalty,
                            -slot_index,
                            max_group_day_load,
                            teacher_day_count.get((item.get("teacherId"), day), 0),
                            day_load_count.get(day, 0),
                            group_day_load_sum,
                        )
                    else:
                        placement_rank = (
                            max_group_day_load,
                            teacher_day_count.get((item.get("teacherId"), day), 0),
                            day_load_count.get(day, 0),
                            group_day_load_sum,
                            slot_index,
                        )
                    placement_candidates.append(
                        (
                            *placement_rank,
                            int(room.get("capacity") or 0),
                            str(room.get("number") or ""),
                            str(room.get("id") or ""),
                            day,
                            hour,
                            room,
                        )
                    )
            placed = None
            if placement_candidates:
                *_, day, hour, room = min(placement_candidates)
                placed = (day, hour, room)
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
            day_load_count[day] = day_load_count.get(day, 0) + 1
            for group_id in [str(group_id) for group_id in item.get("groupIds") or []]:
                group_busy.add((group_id, day, hour))
                group_day_count[(group_id, day)] = group_day_count.get((group_id, day), 0) + 1
                group_day_max_hour[(group_id, day)] = max(
                    group_day_max_hour.get((group_id, day), -1),
                    int(hour),
                )
                if lesson_type == "lecture":
                    lecture_key = (course_id, group_id)
                    slot_index = slot_indexes[(day, hour)]
                    existing_slot_index = lecture_slots_by_course_group.get(lecture_key)
                    if existing_slot_index is None or slot_index < existing_slot_index:
                        lecture_slots_by_course_group[lecture_key] = slot_index
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
    item_by_id = {item.get("id"): item for item in items}
    generated = _rebalance_generated_schedule(
        generated,
        payload,
        item_by_id,
        days,
        hours,
        slot_indexes,
        room_unavailable,
        max_teacher_day,
        max_audience_day,
    )
    return {"status": "GREEDY", "schedule": generated, "diagnostics": {"algorithm": "greedy"}}
