from __future__ import annotations

from collections import defaultdict

from .errors import ApiError

try:
    from ortools.sat.python import cp_model
except ImportError:  # pragma: no cover - depends on environment
    cp_model = None


WEEKDAYS_DEFAULT = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
HOURS_DEFAULT = list(range(8, 18))
LESSON_TYPE_ORDER = {"lecture": 1, "practice": 2, "practical": 2, "seminar": 2, "lab": 3}


def _unique_preserve_order(values):
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _normalize_text(value):
    return str(value).strip().lower() if value is not None else ""


def _normalize_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _normalize_slots(payload):
    time_slots = payload.get("timeSlots") or []
    if time_slots:
        slots = []
        for index, raw in enumerate(time_slots):
            day = raw.get("day")
            hour = raw.get("hour")
            if day is None or hour is None:
                raise ApiError(
                    400,
                    "invalid_time_slot",
                    "Каждый timeSlot должен содержать day и hour.",
                )
            slot_id = raw.get("id") or f"{day}_{hour}"
            slots.append(
                {
                    "id": str(slot_id),
                    "day": str(day),
                    "hour": int(hour),
                    "order": index,
                }
            )
        return slots

    days = payload.get("days") or WEEKDAYS_DEFAULT
    hours = payload.get("hours") or HOURS_DEFAULT
    slots = []
    order = 0
    for day in days:
        for hour in hours:
            slots.append(
                {
                    "id": f"{day}_{hour}",
                    "day": str(day),
                    "hour": int(hour),
                    "order": order,
                }
            )
            order += 1
    return slots


def _normalize_teachers(payload):
    teachers = payload.get("teachers") or []
    normalized = []
    for teacher in teachers:
        teacher_id = teacher.get("id")
        if teacher_id is None:
            raise ApiError(400, "invalid_teacher", "У преподавателя должен быть id.")

        availability = set()
        raw_availability = teacher.get("availability") or []
        for item in raw_availability:
            day = item.get("day")
            hours = item.get("hours") or []
            if day is None:
                continue
            for hour in hours:
                availability.add((str(day), int(hour)))

        normalized.append(
            {
                "id": teacher_id,
                "name": teacher.get("name") or f"Teacher {teacher_id}",
                "availability": availability,
                "max_hours_per_week": teacher.get("maxHoursPerWeek"),
            }
        )
    return normalized


def _normalize_rooms(payload):
    rooms = payload.get("rooms") or []
    normalized = []
    for room in rooms:
        room_id = room.get("id")
        if room_id is None:
            raise ApiError(400, "invalid_room", "У аудитории должен быть id.")

        floor_value = room.get("floor")
        pc_count_value = room.get("pcCount")
        normalized.append(
            {
                "id": room_id,
                "number": room.get("number") or str(room_id),
                "capacity": int(room.get("capacity") or 0),
                "type": _normalize_text(room.get("type")),
                "programme": _normalize_text(room.get("programme") or room.get("department")),
                "building": str(room.get("building") or ""),
                "floor": int(floor_value) if floor_value not in (None, "") else None,
                "pc_count": int(pc_count_value) if pc_count_value not in (None, "") else 0,
            }
        )
    return normalized


def _normalize_plan_items(payload):
    plan_items = payload.get("planItems") or []
    normalized = []
    for index, item in enumerate(plan_items, start=1):
        item_id = item.get("id") or f"item_{index}"
        teacher_id = item.get("teacherId")
        if teacher_id is None:
            raise ApiError(
                400,
                "invalid_plan_item",
                f"У planItem {item_id} должен быть teacherId.",
            )

        lessons_per_week = int(item.get("lessonsPerWeek") or 0)
        if lessons_per_week <= 0:
            raise ApiError(
                400,
                "invalid_plan_item",
                f"У planItem {item_id} lessonsPerWeek должен быть больше 0.",
            )

        group_ids = [str(group_id) for group_id in (item.get("groupIds") or item.get("groups") or [])]
        if not group_ids:
            raise ApiError(
                400,
                "invalid_plan_item",
                f"У planItem {item_id} должен быть хотя бы один groupId.",
            )

        subgroup_ids = [str(subgroup_id) for subgroup_id in (item.get("subgroupIds") or [])]
        stream_id = item.get("streamId")
        stream_id = str(stream_id) if stream_id not in (None, "") else None

        forbidden_slots = set()
        for raw in item.get("forbiddenSlots") or []:
            day = raw.get("day")
            hour = raw.get("hour")
            if day is None or hour is None:
                continue
            forbidden_slots.add((str(day), int(hour)))

        preferred_slots = set()
        for raw in item.get("preferredSlots") or []:
            day = raw.get("day")
            hour = raw.get("hour")
            if day is None or hour is None:
                continue
            preferred_slots.add((str(day), int(hour)))

        lesson_type = _normalize_text(item.get("lessonType") or item.get("type") or "")
        room_type_required = _normalize_text(
            item.get("roomTypeRequired") or item.get("roomType") or ""
        )
        pc_required = _normalize_bool(item.get("pcRequired"))

        if lesson_type == "lab" and not room_type_required:
            room_type_required = "lab"

        if subgroup_ids:
            audience_keys = [f"subgroup:{subgroup_id}" for subgroup_id in subgroup_ids]
        else:
            audience_keys = [f"group:{group_id}" for group_id in group_ids]
        if stream_id and not subgroup_ids:
            audience_keys.append(f"stream:{stream_id}")

        course_key = str(item.get("courseId") or item.get("courseName") or item.get("name") or item_id)
        precedence_signature = (
            course_key,
            tuple(sorted(group_ids)),
            stream_id or "",
        )

        normalized.append(
            {
                "id": str(item_id),
                "course_id": item.get("courseId"),
                "course_key": course_key,
                "course_name": item.get("courseName") or item.get("name") or f"Course {index}",
                "teacher_id": teacher_id,
                "teacher_name": item.get("teacherName") or f"Teacher {teacher_id}",
                "lesson_type": lesson_type or "other",
                "lesson_type_rank": LESSON_TYPE_ORDER.get(lesson_type or "", 99),
                "group_ids": group_ids,
                "subgroup_ids": subgroup_ids,
                "stream_id": stream_id,
                "audience_keys": _unique_preserve_order(audience_keys),
                "lessons_per_week": lessons_per_week,
                "student_count": int(item.get("studentCount") or 0),
                "room_type_required": room_type_required,
                "pc_required": pc_required,
                "programme": _normalize_text(item.get("programme")),
                "preferred_days": {str(day) for day in (item.get("preferredDays") or [])},
                "preferred_hours": {int(hour) for hour in (item.get("preferredHours") or [])},
                "preferred_slots": preferred_slots,
                "preferred_buildings": {str(v) for v in (item.get("preferredBuildings") or [])},
                "forbidden_slots": forbidden_slots,
                "precedence_signature": precedence_signature,
            }
        )
    return normalized


def _slot_score(slot, item):
    score = 0
    if item["preferred_slots"] and (slot["day"], slot["hour"]) in item["preferred_slots"]:
        score += 30
    if item["preferred_days"] and slot["day"] in item["preferred_days"]:
        score += 20
    if item["preferred_hours"] and slot["hour"] in item["preferred_hours"]:
        score += 15

    score += max(0, 18 - int(slot["hour"]))

    if item["lesson_type"] == "lecture" and slot["hour"] <= 12:
        score += 3
    if item["lesson_type"] == "lab" and slot["hour"] >= 10:
        score += 2
    return score


def _room_score(room, item, prefer_lower_floors=True):
    score = 0
    if item["preferred_buildings"] and room["building"] in item["preferred_buildings"]:
        score += 6

    if prefer_lower_floors and room["floor"] is not None:
        score += max(0, 6 - int(room["floor"]))

    if item["room_type_required"] and room["type"] == item["room_type_required"]:
        score += 3
    return score


def _room_matches_item_constraints(room, item, required_type):
    if room["capacity"] and item["student_count"] and room["capacity"] < item["student_count"]:
        return False

    if required_type and required_type not in {"any", "all"}:
        if not room["type"] or room["type"] != required_type:
            return False

    if item["pc_required"]:
        if room["pc_count"] <= 0:
            return False
        if item["student_count"] and room["pc_count"] < item["student_count"]:
            return False
    return True


def _find_compatible_room_ids(item, rooms):
    required_type = item["room_type_required"]
    same_programme = []
    fallback_other_programme = []
    compatible = []
    required_programme = item.get("programme") or ""

    for room in rooms:
        if not _room_matches_item_constraints(room, item, required_type):
            continue

        room_programme = room.get("programme") or ""
        if required_programme:
            if room_programme and room_programme == required_programme:
                same_programme.append(room["id"])
            else:
                fallback_other_programme.append(room["id"])
        else:
            compatible.append(room["id"])

    if required_programme:
        if same_programme:
            return same_programme + fallback_other_programme, False
        return fallback_other_programme, bool(fallback_other_programme)
    return compatible, False


def _group_allowed_slots(item, slots, teacher):
    allowed_slots = []
    for slot in slots:
        slot_key = (slot["day"], slot["hour"])
        if teacher["availability"] and slot_key not in teacher["availability"]:
            continue
        if slot_key in item["forbidden_slots"]:
            continue
        allowed_slots.append(slot["id"])
    return allowed_slots


def optimize_schedule(payload):
    if cp_model is None:
        raise ApiError(
            500,
            "optimizer_dependency_missing",
            "Для оптимизатора нужно установить ortools: pip install ortools",
        )

    teachers = _normalize_teachers(payload)
    rooms = _normalize_rooms(payload)
    items = _normalize_plan_items(payload)
    slots = _normalize_slots(payload)

    if not teachers:
        raise ApiError(400, "optimizer_requires_teachers", "Для оптимизации нужны teachers.")
    if not rooms:
        raise ApiError(400, "optimizer_requires_rooms", "Для оптимизации нужны rooms.")
    if not items:
        raise ApiError(400, "optimizer_requires_plan_items", "Для оптимизации нужны planItems.")
    if not slots:
        raise ApiError(
            400,
            "optimizer_requires_slots",
            "Для оптимизации нужны timeSlots или days/hours.",
        )

    teacher_map = {teacher["id"]: teacher for teacher in teachers}
    room_map = {room["id"]: room for room in rooms}
    slot_map = {slot["id"]: slot for slot in slots}
    slot_index_map = {slot["id"]: int(slot["order"]) for slot in slots}
    max_slot_index = max(slot_index_map.values()) if slot_index_map else 0
    days = _unique_preserve_order(slot["day"] for slot in slots)
    max_classes_per_day_for_teacher = int(payload.get("maxClassesPerDayForTeacher") or 4)
    max_classes_per_day_for_audience = int(payload.get("maxClassesPerDayForAudience") or 4)

    for item in items:
        if item["teacher_id"] not in teacher_map:
            raise ApiError(
                400,
                "unknown_teacher",
                f"Для planItem {item['id']} не найден teacherId={item['teacher_id']}",
            )

    compatible_rooms_by_item = {}
    allowed_slots_by_item = {}
    incompatibilities = []

    for item in items:
        compatible_rooms, _used_programme_fallback = _find_compatible_room_ids(item, rooms)
        if not compatible_rooms:
            incompatibilities.append(
                {
                    "itemId": item["id"],
                    "courseName": item["course_name"],
                    "reason": "Для дисциплины не найдено подходящих аудиторий по типу, вместимости или PCCount.",
                }
            )
            continue
        compatible_rooms_by_item[item["id"]] = compatible_rooms

        teacher = teacher_map[item["teacher_id"]]
        allowed_slots = _group_allowed_slots(item, slots, teacher)
        allowed_slots_by_item[item["id"]] = allowed_slots
        if len(allowed_slots) < item["lessons_per_week"]:
            incompatibilities.append(
                {
                    "itemId": item["id"],
                    "courseName": item["course_name"],
                    "reason": "Недостаточно доступных временных слотов для заданных ограничений.",
                }
            )

    if incompatibilities:
        raise ApiError(
            400,
            "optimizer_input_infeasible",
            "Входные данные несовместимы для оптимизатора.",
            details={"issues": incompatibilities},
        )

    model = cp_model.CpModel()
    decision_vars = {}
    use_slot_vars = {}
    item_day_used_vars = {}
    item_earliest_slot_vars = {}

    prefer_lower_floors = _normalize_bool(payload.get("preferLowerFloors", True))
    enforce_lecture_before_lab = _normalize_bool(payload.get("enforceLectureBeforeLab", True))
    prefer_separate_subgroups_by_day = _normalize_bool(
        payload.get("preferSeparateSubgroupsByDay", False)
    )
    enable_gap_penalties = _normalize_bool(payload.get("enableGapPenalties", True))
    enable_building_transition_penalties = _normalize_bool(
        payload.get("enableBuildingTransitionPenalties", True)
    )

    items_by_id = {item["id"]: item for item in items}

    for item in items:
        item_id = item["id"]
        for slot_id in allowed_slots_by_item[item_id]:
            room_vars_for_slot = []
            for room_id in compatible_rooms_by_item[item_id]:
                variable_name = f"x__{item_id}__{slot_id}__{room_id}"
                var = model.NewBoolVar(variable_name)
                decision_vars[(item_id, slot_id, room_id)] = var
                room_vars_for_slot.append(var)

            use_var = model.NewBoolVar(f"use__{item_id}__{slot_id}")
            use_slot_vars[(item_id, slot_id)] = use_var
            model.Add(sum(room_vars_for_slot) == use_var)

    for item in items:
        item_id = item["id"]
        slot_vars = [
            use_slot_vars[(item_id, slot_id)]
            for slot_id in allowed_slots_by_item[item_id]
            if (item_id, slot_id) in use_slot_vars
        ]
        model.Add(sum(slot_vars) == item["lessons_per_week"])

        for day in days:
            day_slot_vars = [
                use_slot_vars[(item_id, slot_id)]
                for slot_id in allowed_slots_by_item[item_id]
                if slot_map[slot_id]["day"] == day and (item_id, slot_id) in use_slot_vars
            ]
            if not day_slot_vars:
                continue
            day_used = model.NewBoolVar(f"day_used__{item_id}__{day}")
            item_day_used_vars[(item_id, day)] = day_used
            model.Add(sum(day_slot_vars) >= day_used)
            model.Add(sum(day_slot_vars) <= len(day_slot_vars) * day_used)

        transformed_index_vars = []
        for slot_id in allowed_slots_by_item[item_id]:
            use_var = use_slot_vars[(item_id, slot_id)]
            idx_var = model.NewIntVar(0, max_slot_index + 1, f"idx__{item_id}__{slot_id}")
            model.Add(idx_var == slot_index_map[slot_id]).OnlyEnforceIf(use_var)
            model.Add(idx_var == max_slot_index + 1).OnlyEnforceIf(use_var.Not())
            transformed_index_vars.append(idx_var)

        earliest_var = model.NewIntVar(0, max_slot_index + 1, f"earliest__{item_id}")
        model.AddMinEquality(earliest_var, transformed_index_vars)
        item_earliest_slot_vars[item_id] = earliest_var

    for room in rooms:
        room_id = room["id"]
        for slot in slots:
            room_slot_vars = [
                var
                for (_item_id, slot_id, current_room_id), var in decision_vars.items()
                if current_room_id == room_id and slot_id == slot["id"]
            ]
            if room_slot_vars:
                model.Add(sum(room_slot_vars) <= 1)

    items_by_teacher = defaultdict(list)
    for item in items:
        items_by_teacher[item["teacher_id"]].append(item["id"])

    for teacher in teachers:
        teacher_item_ids = set(items_by_teacher[teacher["id"]])
        for slot in slots:
            teacher_slot_vars = [
                use_slot_vars[(item_id, slot["id"])]
                for item_id in teacher_item_ids
                if (item_id, slot["id"]) in use_slot_vars
            ]
            if teacher_slot_vars:
                model.Add(sum(teacher_slot_vars) <= 1)

        for day in days:
            teacher_day_vars = [
                use_slot_vars[(item_id, slot["id"])]
                for item_id in teacher_item_ids
                for slot in slots
                if slot["day"] == day and (item_id, slot["id"]) in use_slot_vars
            ]
            if teacher_day_vars:
                model.Add(sum(teacher_day_vars) <= max_classes_per_day_for_teacher)

        max_hours = teacher.get("max_hours_per_week")
        if max_hours:
            teacher_vars = [
                var for (item_id, _slot_id), var in use_slot_vars.items() if item_id in teacher_item_ids
            ]
            if teacher_vars:
                model.Add(sum(teacher_vars) <= int(max_hours))

    audience_to_items = defaultdict(list)
    for item in items:
        for audience_key in item["audience_keys"]:
            audience_to_items[audience_key].append(item["id"])

    for _audience_key, audience_item_ids in audience_to_items.items():
        item_id_set = set(audience_item_ids)
        for slot in slots:
            audience_slot_vars = [
                use_slot_vars[(item_id, slot["id"])]
                for item_id in item_id_set
                if (item_id, slot["id"]) in use_slot_vars
            ]
            if audience_slot_vars:
                model.Add(sum(audience_slot_vars) <= 1)
        for day in days:
            audience_day_vars = [
                use_slot_vars[(item_id, slot["id"])]
                for item_id in item_id_set
                for slot in slots
                if slot["day"] == day and (item_id, slot["id"]) in use_slot_vars
            ]
            if audience_day_vars:
                model.Add(sum(audience_day_vars) <= max_classes_per_day_for_audience)

    precedence_relations = []
    if enforce_lecture_before_lab:
        lecture_items = [item for item in items if item["lesson_type"] == "lecture"]
        later_items = [
            item
            for item in items
            if item["lesson_type"] in {"practice", "practical", "seminar", "lab"}
        ]

        for later_item in later_items:
            matching_lectures = [
                lecture_item
                for lecture_item in lecture_items
                if lecture_item["course_key"] == later_item["course_key"]
                and set(lecture_item["group_ids"]) & set(later_item["group_ids"])
            ]
            if not matching_lectures:
                continue

            lecture_earliest = model.NewIntVar(0, max_slot_index + 1, "earliest_lecture_group")
            model.AddMinEquality(
                lecture_earliest,
                [item_earliest_slot_vars[item["id"]] for item in matching_lectures],
            )
            model.Add(lecture_earliest < item_earliest_slot_vars[later_item["id"]])
            precedence_relations.append(
                {
                    "beforeItemId": matching_lectures[0]["id"],
                    "beforeLessonType": "lecture",
                    "afterItemId": later_item["id"],
                    "afterLessonType": later_item["lesson_type"],
                    "courseName": later_item["course_name"],
                }
            )
    subgroup_day_separation_pairs = []
    subgroup_same_day_penalty_vars = []
    teacher_gap_penalty_vars = []
    audience_gap_penalty_vars = []
    teacher_building_penalty_vars = []
    audience_building_penalty_vars = []

    if prefer_separate_subgroups_by_day:
        subgroup_items_by_signature = defaultdict(list)

        for item in items:
            if not item["subgroup_ids"]:
                continue
            subgroup_signature = (
                item["course_key"],
                tuple(sorted(item["group_ids"])),
                item["teacher_id"],
            )
            subgroup_items_by_signature[subgroup_signature].append(item)

        for signature_items in subgroup_items_by_signature.values():
            if len(signature_items) < 2:
                continue

            for day in days:
                day_item_vars = []
                for item in signature_items:
                    day_used_var = item_day_used_vars.get((item["id"], day))
                    if day_used_var is not None:
                        day_item_vars.append(day_used_var)

                if len(day_item_vars) >= 2:
                    overlap_var = model.NewIntVar(0, len(day_item_vars), f"subgroup_overlap__{day}")
                    model.Add(overlap_var == sum(day_item_vars))
                    penalty_var = model.NewIntVar(0, len(day_item_vars), f"subgroup_penalty__{day}")
                    model.AddMaxEquality(
                        penalty_var,
                        [overlap_var - 1, model.NewConstant(0)],
                    )
                    subgroup_same_day_penalty_vars.append(penalty_var)
                    subgroup_day_separation_pairs.append(
                        {
                            "courseName": signature_items[0]["course_name"],
                            "teacherId": signature_items[0]["teacher_id"],
                            "day": day,
                            "subgroupItemIds": [item["id"] for item in signature_items],
                        }
                    )

    slots_by_day = defaultdict(list)
    for slot in slots:
        slots_by_day[slot["day"]].append(slot)
    for day_slots in slots_by_day.values():
        day_slots.sort(key=lambda slot: int(slot["hour"]))

    if enable_gap_penalties:
        for teacher in teachers:
            teacher_item_ids = set(items_by_teacher[teacher["id"]])
            for day, day_slots in slots_by_day.items():
                for left_index in range(len(day_slots)):
                    for right_index in range(left_index + 1, len(day_slots)):
                        left_slot = day_slots[left_index]
                        right_slot = day_slots[right_index]
                        gap_size = int(right_slot["hour"]) - int(left_slot["hour"]) - 1
                        if gap_size <= 0:
                            continue

                        left_vars = [
                            use_slot_vars[(item_id, left_slot["id"])]
                            for item_id in teacher_item_ids
                            if (item_id, left_slot["id"]) in use_slot_vars
                        ]
                        right_vars = [
                            use_slot_vars[(item_id, right_slot["id"])]
                            for item_id in teacher_item_ids
                            if (item_id, right_slot["id"]) in use_slot_vars
                        ]
                        if not left_vars or not right_vars:
                            continue

                        left_used = model.NewBoolVar(
                            f"teacher_left_used__{teacher['id']}__{day}__{left_slot['hour']}"
                        )
                        right_used = model.NewBoolVar(
                            f"teacher_right_used__{teacher['id']}__{day}__{right_slot['hour']}"
                        )
                        model.AddMaxEquality(left_used, left_vars)
                        model.AddMaxEquality(right_used, right_vars)

                        gap_var = model.NewBoolVar(
                            f"teacher_gap__{teacher['id']}__{day}__{left_slot['hour']}__{right_slot['hour']}"
                        )
                        model.Add(gap_var <= left_used)
                        model.Add(gap_var <= right_used)
                        model.Add(gap_var >= left_used + right_used - 1)
                        teacher_gap_penalty_vars.extend([gap_var] * gap_size)

    def _collect_building_choice_vars(item_ids, slot_id):
        result = defaultdict(list)
        for (item_id, current_slot_id, room_id), var in decision_vars.items():
            if item_id in item_ids and current_slot_id == slot_id:
                building = room_map[room_id]["building"] or ""
                result[building].append(var)
        return result

    def _building_used_var(prefix, item_ids, slot_id, building):
        building_vars = [
            var
            for (item_id, current_slot_id, room_id), var in decision_vars.items()
            if item_id in item_ids
            and current_slot_id == slot_id
            and (room_map[room_id]["building"] or "") == building
        ]
        if not building_vars:
            return None
        used_var = model.NewBoolVar(f"{prefix}__{slot_id}__{building or 'empty'}")
        model.AddMaxEquality(used_var, building_vars)
        return used_var

    if enable_building_transition_penalties:
        for teacher in teachers:
            teacher_item_ids = set(items_by_teacher[teacher["id"]])
            for day, day_slots in slots_by_day.items():
                for left_slot, right_slot in zip(day_slots, day_slots[1:]):
                    left_buildings = _collect_building_choice_vars(teacher_item_ids, left_slot["id"])
                    right_buildings = _collect_building_choice_vars(teacher_item_ids, right_slot["id"])
                    if not left_buildings or not right_buildings:
                        continue
                    for left_building, left_vars in left_buildings.items():
                        for right_building, right_vars in right_buildings.items():
                            if left_building == right_building:
                                continue
                            left_used = _building_used_var(
                                f"teacher_building_left__{teacher['id']}__{day}__{left_slot['hour']}",
                                teacher_item_ids,
                                left_slot["id"],
                                left_building,
                            )
                            right_used = _building_used_var(
                                f"teacher_building_right__{teacher['id']}__{day}__{right_slot['hour']}",
                                teacher_item_ids,
                                right_slot["id"],
                                right_building,
                            )
                            if left_used is None or right_used is None:
                                continue
                            transition_var = model.NewBoolVar(
                                f"teacher_building_transition__{teacher['id']}__{day}__{left_slot['hour']}__{right_slot['hour']}__{left_building}__{right_building}"
                            )
                            model.Add(transition_var <= left_used)
                            model.Add(transition_var <= right_used)
                            model.Add(transition_var >= left_used + right_used - 1)
                            teacher_building_penalty_vars.append(transition_var)

    if enable_gap_penalties or enable_building_transition_penalties:
        for audience_key, audience_item_ids in audience_to_items.items():
            item_id_set = set(audience_item_ids)
            for day, day_slots in slots_by_day.items():
                if enable_gap_penalties:
                    for left_index in range(len(day_slots)):
                        for right_index in range(left_index + 1, len(day_slots)):
                            left_slot = day_slots[left_index]
                            right_slot = day_slots[right_index]
                            gap_size = int(right_slot["hour"]) - int(left_slot["hour"]) - 1
                            if gap_size <= 0:
                                continue

                            left_vars = [
                                use_slot_vars[(item_id, left_slot["id"])]
                                for item_id in item_id_set
                                if (item_id, left_slot["id"]) in use_slot_vars
                            ]
                            right_vars = [
                                use_slot_vars[(item_id, right_slot["id"])]
                                for item_id in item_id_set
                                if (item_id, right_slot["id"]) in use_slot_vars
                            ]
                            if not left_vars or not right_vars:
                                continue

                            left_used = model.NewBoolVar(
                                f"aud_left_used__{audience_key}__{day}__{left_slot['hour']}"
                            )
                            right_used = model.NewBoolVar(
                                f"aud_right_used__{audience_key}__{day}__{right_slot['hour']}"
                            )
                            model.AddMaxEquality(left_used, left_vars)
                            model.AddMaxEquality(right_used, right_vars)

                            gap_var = model.NewBoolVar(
                                f"aud_gap__{audience_key}__{day}__{left_slot['hour']}__{right_slot['hour']}"
                            )
                            model.Add(gap_var <= left_used)
                            model.Add(gap_var <= right_used)
                            model.Add(gap_var >= left_used + right_used - 1)
                            audience_gap_penalty_vars.extend([gap_var] * gap_size)

                if enable_building_transition_penalties:
                    for left_slot, right_slot in zip(day_slots, day_slots[1:]):
                        left_buildings = _collect_building_choice_vars(item_id_set, left_slot["id"])
                        right_buildings = _collect_building_choice_vars(item_id_set, right_slot["id"])
                        if not left_buildings or not right_buildings:
                            continue
                        for left_building in left_buildings:
                            for right_building in right_buildings:
                                if left_building == right_building:
                                    continue
                                left_used = _building_used_var(
                                    f"aud_building_left__{audience_key}__{day}__{left_slot['hour']}",
                                    item_id_set,
                                    left_slot["id"],
                                    left_building,
                                )
                                right_used = _building_used_var(
                                    f"aud_building_right__{audience_key}__{day}__{right_slot['hour']}",
                                    item_id_set,
                                    right_slot["id"],
                                    right_building,
                                )
                                if left_used is None or right_used is None:
                                    continue
                                transition_var = model.NewBoolVar(
                                    f"aud_building_transition__{audience_key}__{day}__{left_slot['hour']}__{right_slot['hour']}__{left_building}__{right_building}"
                                )
                                model.Add(transition_var <= left_used)
                                model.Add(transition_var <= right_used)
                                model.Add(transition_var >= left_used + right_used - 1)
                                audience_building_penalty_vars.append(transition_var)

    objective_terms = []
    for (item_id, slot_id, room_id), var in decision_vars.items():
        item = items_by_id[item_id]
        room = room_map[room_id]
        slot = slot_map[slot_id]

        coefficient = _slot_score(slot, item) * 10
        coefficient += _room_score(room, item, prefer_lower_floors=prefer_lower_floors)
        if item.get("programme"):
            if (room.get("programme") or "") == item["programme"]:
                coefficient += 40
            else:
                coefficient -= 20
        objective_terms.append(coefficient * var)

    if subgroup_same_day_penalty_vars:
        penalty_weight = 25
        objective_terms.extend(-penalty_weight * var for var in subgroup_same_day_penalty_vars)
    if teacher_gap_penalty_vars:
        objective_terms.extend(-12 * var for var in teacher_gap_penalty_vars)
    if audience_gap_penalty_vars:
        objective_terms.extend(-14 * var for var in audience_gap_penalty_vars)
    if teacher_building_penalty_vars:
        objective_terms.extend(-10 * var for var in teacher_building_penalty_vars)
    if audience_building_penalty_vars:
        objective_terms.extend(-8 * var for var in audience_building_penalty_vars)

    model.Maximize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(payload.get("maxSolveTimeSeconds", 10))
    solver.parameters.num_search_workers = int(payload.get("numWorkers", 8))

    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        raise ApiError(
            400,
            "optimizer_no_solution",
            "Не удалось найти допустимое расписание для заданных ограничений.",
        )

    schedule = []
    room_usage = defaultdict(list)
    teacher_usage = defaultdict(list)
    audience_usage = defaultdict(list)
    programme_fallback_scheduled_items = set()

    for (item_id, slot_id, room_id), var in decision_vars.items():
        if solver.Value(var) != 1:
            continue

        item = items_by_id[item_id]
        slot = slot_map[slot_id]
        room = room_map[room_id]
        teacher = teacher_map[item["teacher_id"]]

        entry = {
            "itemId": item_id,
            "courseId": item["course_id"],
            "courseName": item["course_name"],
            "lessonType": item["lesson_type"],
            "teacherId": teacher["id"],
            "teacherName": teacher["name"],
            "roomId": room["id"],
            "roomNumber": room["number"],
            "roomType": room["type"],
            "roomProgramme": room.get("programme") or "",
            "requestedProgramme": item.get("programme") or "",
            "roomProgrammeFallbackUsed": bool(
                item.get("programme")
                and (room.get("programme") or "") != item.get("programme")
            ),
            "groups": item["group_ids"],
            "subgroups": item["subgroup_ids"],
            "streamId": item["stream_id"],
            "day": slot["day"],
            "hour": slot["hour"],
        }
        schedule.append(entry)
        if entry["roomProgrammeFallbackUsed"]:
            programme_fallback_scheduled_items.add(item_id)

        room_usage[room["id"]].append(f"{slot['day']} {slot['hour']}:00")
        teacher_usage[teacher["id"]].append(f"{slot['day']} {slot['hour']}:00")
        for audience_key in item["audience_keys"]:
            audience_usage[audience_key].append(f"{slot['day']} {slot['hour']}:00")

    schedule.sort(key=lambda entry: (days.index(entry["day"]), entry["hour"], entry["courseName"]))

    diagnostics = {
        "objectiveValue": solver.ObjectiveValue(),
        "status": "OPTIMAL" if status == cp_model.OPTIMAL else "FEASIBLE",
        "roomUsage": {str(room_id): usage for room_id, usage in room_usage.items()},
        "teacherUsage": {str(teacher_id): usage for teacher_id, usage in teacher_usage.items()},
        "audienceUsage": {audience_key: usage for audience_key, usage in audience_usage.items()},
        "precedenceRelations": precedence_relations,
        "preferLowerFloors": prefer_lower_floors,
        "enableGapPenalties": enable_gap_penalties,
        "enableBuildingTransitionPenalties": enable_building_transition_penalties,
        "maxClassesPerDayForTeacher": max_classes_per_day_for_teacher,
        "maxClassesPerDayForAudience": max_classes_per_day_for_audience,
        "preferSeparateSubgroupsByDay": prefer_separate_subgroups_by_day,
        "subgroupSeparationPairs": len(subgroup_day_separation_pairs),
        "programmeFallbackItems": sorted(programme_fallback_scheduled_items),
    }

    return {
        "status": diagnostics["status"],
        "schedule": schedule,
        "diagnostics": diagnostics,
    }
