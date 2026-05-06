from calendar import monthrange
from copy import deepcopy
from datetime import date, datetime, timedelta

from sqlalchemy import delete, func, or_, select, update

from ..core.orm import SessionLocal
from ..models import Course, Group, Room, RoomBlock, Schedule, Section, Teacher, TeacherPreferenceRequest
from ..programmes.education import get_home_room_programmes
from ..core.errors import ApiError
from ..programmes.utils import normalize_programme_text
from .config import CP_SAT_WARM_START_ENABLED, normalize_schedule_algorithm
from .cp_sat import optimize_cpsat_schedule
from .cp_sat.cp_sat_fast import optimize_cpsat_fast_schedule
from .greedy import optimize_greedy_schedule
from .mix import optimize_cpsat_greedy_schedule
from .payload import (
    _build_optimizer_payload,
    _chunk_sections_by_capacity,
    _int_at_least,
    _is_first_year_section,
    _is_physical_education,
    _lecture_stream_id,
    _max_room_capacity_for_lesson,
    _resolve_subgroup_count,
    _subgroup_label,
)

DAY_NAME_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
}
SEASON_ACADEMIC_PERIODS = {
    1: (1, 3, 5, 7),
    2: (2, 4, 6, 8),
}


def monday_for_week(target_year):
    today = date.today()
    safe_day = min(today.day, monthrange(target_year, today.month)[1])
    anchor = date(target_year, today.month, safe_day)
    return anchor - timedelta(days=anchor.weekday())


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


def _get_room_blocked_slots(session, semester=None, year=None):
    conditions = []
    if semester is not None:
        conditions.append(or_(RoomBlock.semester == semester, RoomBlock.semester.is_(None)))
    if year is not None:
        conditions.append(or_(RoomBlock.year == year, RoomBlock.year.is_(None)))

    statement = select(
        RoomBlock.room_id.label("room_id"),
        RoomBlock.day.label("day"),
        RoomBlock.start_hour.label("start_hour"),
        RoomBlock.end_hour.label("end_hour"),
    )
    if conditions:
        statement = statement.where(*conditions)
    rows = session.execute(
        statement.order_by(RoomBlock.room_id, RoomBlock.day, RoomBlock.start_hour)
    ).mappings().all()

    blocked_by_room = {}
    for row in rows:
        room_id = row.get("room_id")
        day = _room_block_day_for_optimizer(row.get("day"))
        start_hour = row.get("start_hour")
        end_hour = row.get("end_hour")
        if not room_id or not day or start_hour in (None, ""):
            continue
        start_value = int(start_hour)
        end_value = int(end_hour) if end_hour not in (None, "") else start_value + 1
        if end_value <= start_value:
            end_value = start_value + 1
        room_slots = blocked_by_room.setdefault(room_id, set())
        for hour in range(start_value, end_value):
            room_slots.add((day, hour))
    return blocked_by_room


def _recompute_room_availability(session):
    session.execute(
        update(Room)
        .where(Room.available.is_(None))
        .values(available=1)
    )


def _schedule_to_dict(row):
    return {
        "id": row.id,
        "section_id": row.section_id,
        "course_id": row.course_id,
        "course_name": row.course_name,
        "teacher_id": row.teacher_id,
        "teacher_name": row.teacher_name,
        "room_id": row.room_id,
        "room_number": row.room_number,
        "group_id": row.group_id,
        "group_name": row.group_name,
        "subgroup": row.subgroup,
        "day": row.day,
        "start_hour": row.start_hour,
        "semester": row.semester,
        "year": row.year,
        "algorithm": row.algorithm,
        "room_programme": row.room_programme,
        "room_programme_mismatch": row.room_programme_mismatch,
        "relocated_from_room_number": row.relocated_from_room_number,
        "relocation_reason": row.relocation_reason,
    }


def _schedule_from_generated_row(row):
    return Schedule(
        section_id=row[0],
        course_id=row[1],
        course_name=row[2],
        teacher_id=row[3],
        teacher_name=row[4],
        room_id=row[5],
        room_number=row[6],
        group_id=row[7],
        group_name=row[8],
        subgroup=row[9],
        day=row[10],
        start_hour=row[11],
        semester=row[12],
        year=row[13],
        algorithm=row[14],
        room_programme=row[15],
        room_programme_mismatch=row[16],
        relocated_from_room_number=row[17],
        relocation_reason=row[18],
    )


def academic_periods_for_schedule_semester(semester):
    return SEASON_ACADEMIC_PERIODS.get(int(semester), (int(semester),))


def _build_section_lookup(sections, rooms):
    section_lookup = {}
    for section in sections:
        lesson_type = (section.get("lesson_type") or "lecture").strip().lower()
        if lesson_type == "lecture" and _is_first_year_section(section):
            continue
        section_lookup[f"section_{section['id']}"] = [
            {
                "section_id": section["id"],
                "group_id": section["group_id"],
                "group_name": section["group_name"],
                "subgroup": "",
            }
        ]
        if lesson_type == "lecture":
            continue
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
        if lesson_type != "lecture" or not _is_first_year_section(section):
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
    max_lecture_capacity = _max_room_capacity_for_lesson(rooms, "lecture")
    for _signature, lecture_sections in lecture_groups.items():
        for lecture_chunk in _chunk_sections_by_capacity(lecture_sections, max_lecture_capacity):
            item_id = _lecture_stream_id(lecture_chunk)
            section_lookup[item_id] = [
                {
                    "section_id": section["id"],
                    "group_id": section["group_id"],
                    "group_name": section["group_name"],
                    "subgroup": "",
                }
                for section in lecture_chunk
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


def _schedule_batch_key(section):
    return (
        int(section.get("study_course") or 0),
        str(section.get("group_language") or "").strip().lower(),
        normalize_programme_text(section.get("group_programme") or section.get("programme") or ""),
        normalize_programme_text(section.get("specialty_code") or ""),
    )


def _generate_schedule_rows_by_batches(
    sections,
    teachers,
    rooms,
    teacher_preferences,
    semester,
    year,
    algorithm,
    progress_callback=None,
):
    selected_algorithm = normalize_schedule_algorithm(algorithm)
    selected_monday = monday_for_week(year)
    rows = []
    occupied = {"rooms": {}, "teachers": {}, "groups": {}}
    batch_keys = sorted(
        {_schedule_batch_key(section) for section in sections},
        key=lambda key: (-key[0], key[2], key[1], key[3]),
    )
    total_batches = len(batch_keys)
    for batch_index, batch_key in enumerate(batch_keys, start=1):
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
        if progress_callback:
            progress_callback(
                {
                    "stage": "batch_started",
                    "message": "Генерируется пакет расписания",
                    "currentBatch": batch_index,
                    "totalBatches": total_batches,
                    "batchSections": len(batch_sections),
                    "batchPlanItems": len(payload.get("planItems") or []),
                }
            )
        if selected_algorithm in {"cpsat", "cpsat_fast", "hybrid"}:
            if CP_SAT_WARM_START_ENABLED:
                try:
                    payload["warmStartSchedule"] = optimize_greedy_schedule(payload).get("schedule") or []
                except ApiError:
                    payload["warmStartSchedule"] = []
        if selected_algorithm == "greedy":
            optimization_result = optimize_greedy_schedule(payload)
        elif selected_algorithm == "cpsat":
            optimization_result = optimize_cpsat_schedule(
                payload,
                context={
                    "study_course": study_course,
                    "batch_key": {
                        "language": batch_key[1],
                        "programme": batch_key[2],
                        "specialtyCode": batch_key[3],
                    },
                    "batch_sections_count": len(batch_sections),
                },
            )
        elif selected_algorithm == "cpsat_fast":
            optimization_result = optimize_cpsat_fast_schedule(
                payload,
                context={
                    "study_course": study_course,
                    "batch_key": {
                        "language": batch_key[1],
                        "programme": batch_key[2],
                        "specialtyCode": batch_key[3],
                    },
                    "batch_sections_count": len(batch_sections),
                },
            )
        else:
            try:
                optimization_result = optimize_cpsat_greedy_schedule(
                    payload,
                    context={
                        "study_course": study_course,
                        "batch_key": {
                            "language": batch_key[1],
                            "programme": batch_key[2],
                            "specialtyCode": batch_key[3],
                        },
                        "batch_sections_count": len(batch_sections),
                    },
                )
            except ApiError as exc:
                raise ApiError(
                    exc.status,
                    exc.code,
                    f"{exc.message} Пакет: {study_course} курс, {batch_key[2] or 'без направления'}, {batch_key[1] or 'без языка'}.",
                    details=getattr(exc, "details", None) or {},
                ) from exc
        generated_items = optimization_result.get("schedule") or []
        if progress_callback:
            progress_callback(
                {
                    "stage": "batch_completed",
                    "message": "Пакет расписания готов",
                    "currentBatch": batch_index,
                    "totalBatches": total_batches,
                    "batchSections": len(batch_sections),
                    "batchPlanItems": len(payload.get("planItems") or []),
                    "generatedItems": len(generated_items),
                }
            )
        section_lookup = _build_section_lookup(batch_sections, rooms)
        rows.extend(_rows_from_generated_items(generated_items, section_lookup, selected_monday, semester, year, selected_algorithm))
        _record_batch_occupancy(occupied, generated_items)
    return rows


def build_schedule(connection, semester, year, algorithm, progress_callback=None):
    algorithm = normalize_schedule_algorithm(algorithm)
    academic_periods = academic_periods_for_schedule_semester(semester)
    with SessionLocal() as session:
        sections = [
            dict(row)
            for row in session.execute(
                select(
                    Section.id.label("id"),
                    Section.course_id.label("course_id"),
                    Section.course_name.label("course_name"),
                    Course.code.label("course_code"),
                    Section.group_id.label("group_id"),
                    Section.group_name.label("group_name"),
                    Section.classes_count.label("classes_count"),
                    Section.lesson_type.label("lesson_type"),
                    Section.subgroup_mode.label("subgroup_mode"),
                    Section.subgroup_count.label("subgroup_count"),
                    Section.requires_computers.label("requires_computers"),
                    func.coalesce(Section.teacher_id, Course.instructor_id).label("instructor_id"),
                    func.coalesce(
                        func.nullif(Section.teacher_name, ""),
                        Course.instructor_name,
                        "",
                    ).label("instructor_name"),
                    Course.department.label("department"),
                    Course.programme.label("programme"),
                    Course.semester.label("semester"),
                    Course.year.label("year"),
                    Group.student_count.label("student_count"),
                    Group.has_subgroups.label("has_subgroups"),
                    Group.language.label("group_language"),
                    Group.study_course.label("study_course"),
                    Group.programme.label("group_programme"),
                    Group.specialty_code.label("specialty_code"),
                )
                .join(Course, Course.id == Section.course_id)
                .join(Group, Group.id == Section.group_id)
                .where(
                    Course.semester.in_(academic_periods),
                    Section.lesson_type.in_(("lecture", "practical", "lab")),
                )
                .order_by(Group.student_count.desc(), Section.classes_count.desc(), Section.id)
            ).mappings().all()
        ]
        teachers = [
            dict(row)
            for row in session.execute(
                select(
                    Teacher.id.label("id"),
                    Teacher.name.label("name"),
                    Teacher.weekly_hours_limit.label("weekly_hours_limit"),
                    Teacher.teaching_languages.label("teaching_languages"),
                ).order_by(Teacher.id)
            ).mappings().all()
        ]
        rooms = [
            {**dict(row), "building": ""}
            for row in session.execute(
                select(
                    Room.id.label("id"),
                    Room.number.label("number"),
                    Room.capacity.label("capacity"),
                    Room.available.label("available"),
                    Room.type.label("type"),
                    Room.programme.label("programme"),
                    Room.computer_count.label("computer_count"),
                )
                .where(func.coalesce(Room.available, 1) == 1)
                .order_by(Room.capacity, Room.id)
            ).mappings().all()
        ]
        room_blocked_slots = _get_room_blocked_slots(session, semester, year)
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

        preflight_issues = []
        teacher_by_id = {teacher["id"]: teacher for teacher in teachers}

        for section in sections:
            if not section.get("instructor_id"):
                preflight_issues.append(
                    {
                        "type": "teacher_missing",
                        "sectionId": section.get("id"),
                        "courseId": section.get("course_id"),
                        "courseCode": section.get("course_code"),
                        "courseName": section.get("course_name"),
                        "groupId": section.get("group_id"),
                        "groupName": section.get("group_name"),
                        "lessonType": section.get("lesson_type"),
                        "reason": f"Для курса '{section['course_name']}' не найден преподаватель.",
                    }
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
                preflight_issues.append(
                    {
                        "type": "study_course_missing",
                        "sectionId": section.get("id"),
                        "courseId": section.get("course_id"),
                        "courseCode": section.get("course_code"),
                        "courseName": section.get("course_name"),
                        "groupId": section.get("group_id"),
                        "groupName": section.get("group_name"),
                        "reason": f"Для группы '{section['group_name']}' не указан курс обучения.",
                    }
                )
                continue
            if int(section.get("study_course")) != int(section.get("year") or 0):
                preflight_issues.append(
                    {
                        "type": "study_course_mismatch",
                        "sectionId": section.get("id"),
                        "courseId": section.get("course_id"),
                        "courseCode": section.get("course_code"),
                        "courseName": section.get("course_name"),
                        "courseYear": section.get("year"),
                        "groupId": section.get("group_id"),
                        "groupName": section.get("group_name"),
                        "groupStudyCourse": section.get("study_course"),
                        "reason": f"Дисциплина '{section['course_name']}' предназначена для {section['year']} курса, а группа '{section['group_name']}' указана как {section['study_course']} курс.",
                    }
                )
            group_language = str(section.get("group_language") or "ru").strip().lower()
            teacher_languages = teacher_language_map.get(section["instructor_id"], {"ru", "kk"})
            if section.get("instructor_id") and group_language not in teacher_languages:
                teacher = teacher_by_id.get(section["instructor_id"], {})
                preflight_issues.append(
                    {
                        "type": "teacher_language_mismatch",
                        "sectionId": section.get("id"),
                        "courseId": section.get("course_id"),
                        "courseCode": section.get("course_code"),
                        "courseName": section.get("course_name"),
                        "groupId": section.get("group_id"),
                        "groupName": section.get("group_name"),
                        "groupLanguage": group_language,
                        "teacherId": section.get("instructor_id"),
                        "teacherName": section.get("instructor_name") or teacher.get("name"),
                        "teacherLanguages": sorted(teacher_languages),
                        "field": "teaching_languages",
                        "reason": f"Преподаватель курса '{section['course_name']}' не поддерживает язык группы '{group_language}'.",
                    }
                )

        if preflight_issues:
            raise ApiError(
                400,
                "schedule_preflight_failed",
                "Перед генерацией найдены ошибки в данных.",
                details={
                    "semester": semester,
                    "academicPeriods": list(academic_periods),
                    "year": year,
                    "issues": preflight_issues,
                },
            )

        teacher_preference_rows = session.execute(
            select(
                TeacherPreferenceRequest.teacher_id.label("teacher_id"),
                TeacherPreferenceRequest.preferred_day.label("preferred_day"),
                TeacherPreferenceRequest.preferred_hour.label("preferred_hour"),
            )
            .where(TeacherPreferenceRequest.status == "approved")
            .order_by(
                TeacherPreferenceRequest.teacher_id,
                TeacherPreferenceRequest.preferred_day,
                TeacherPreferenceRequest.preferred_hour,
            )
        ).mappings().all()
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
            progress_callback=progress_callback,
        )

        generated_group_ids = sorted({int(section["group_id"]) for section in sections if section.get("group_id") is not None})

        session.execute(
            delete(Schedule).where(
                Schedule.semester == semester,
                Schedule.year == year,
            )
        )
        session.add_all(_schedule_from_generated_row(row) for row in rows)

        if generated_group_ids:
            session.execute(
                update(Group)
                .where(Group.id.in_(generated_group_ids))
                .values(has_subgroups=0)
            )

        generated_subgroup_group_ids = sorted(
            {
                int(row[7])
                for row in rows
                if str(row[9] or "").strip()
            }
        )
        if generated_subgroup_group_ids:
            session.execute(
                update(Group)
                .where(Group.id.in_(generated_subgroup_group_ids))
                .values(has_subgroups=1)
            )

        _recompute_room_availability(session)
        session.commit()

        return [
            _schedule_to_dict(row)
            for row in session.scalars(
                select(Schedule)
                .where(Schedule.semester == semester, Schedule.year == year)
                .order_by(Schedule.day, Schedule.start_hour, Schedule.id)
            ).all()
        ]
