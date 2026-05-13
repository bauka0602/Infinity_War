from math import ceil

from sqlalchemy import delete, func, or_, select, update

from ..core.orm import SessionLocal
from ..models import (
    Course,
    CourseComponent,
    Group,
    IupEntry,
    Notification,
    Room,
    RoomBlock,
    Schedule,
    Section,
    Student,
    Teacher,
    TeacherPreferenceRequest,
    User,
)
from ..programmes.education import (
    get_home_room_programmes,
    resolve_education_group_value,
    room_matches_home_programmes,
)
from ..core.errors import ApiError
from ..notifications.service import create_schedule_change_notifications
from ..programmes.utils import same_programme
from ..rooms.availability import (
    normalize_room_block_day,
    recompute_room_availability,
)
from ..teachers.utils import build_teacher_name_signature, normalize_teacher_name
from .normalization import (
    MIN_COMPUTER_COUNT,
    infer_group_entry_year,
    infer_study_course,
    is_physical_education_course,
    is_physical_education_room,
    normalize_language,
    normalize_lesson_type,
    normalize_number_fields,
    normalize_programme,
    normalize_room_block_interval,
    normalize_room_type,
    normalize_specialty,
    normalize_subgroup,
    normalize_subgroup_mode,
    normalize_teaching_languages,
    positive_int,
    schedule_room_type_matches,
    section_requires_computers,
    validate_teacher_email,
)


def _teacher_disciplines_map(connection):
    disciplines_by_teacher = {}
    with SessionLocal() as session:
        sources = [
            session.execute(
                select(CourseComponent.teacher_id, CourseComponent.course_name)
                .where(
                    CourseComponent.teacher_id.is_not(None),
                    func.trim(func.coalesce(CourseComponent.course_name, "")) != "",
                )
            ).all(),
            session.execute(
                select(Course.instructor_id, Course.name)
                .where(
                    Course.instructor_id.is_not(None),
                    func.trim(func.coalesce(Course.name, "")) != "",
                )
            ).all(),
            session.execute(
                select(Section.teacher_id, Section.course_name)
                .where(
                    Section.teacher_id.is_not(None),
                    func.trim(func.coalesce(Section.course_name, "")) != "",
                )
            ).all(),
            session.execute(
                select(Teacher.id, Teacher.subject_taught)
                .where(func.trim(func.coalesce(Teacher.subject_taught, "")) != "")
            ).all(),
        ]
    for rows in sources:
        for teacher_id, discipline_name in rows:
            if not teacher_id:
                continue
            disciplines = disciplines_by_teacher.setdefault(teacher_id, [])
            for item in str(discipline_name or "").replace(";", ",").split(","):
                discipline = item.strip()
                if discipline and discipline not in disciplines:
                    disciplines.append(discipline)
    for disciplines in disciplines_by_teacher.values():
        disciplines.sort()
    return disciplines_by_teacher


def _serialize_teacher(row, disciplines_by_teacher=None):
    disciplines = list((disciplines_by_teacher or {}).get(row["id"], []))
    return {
        **row,
        "assigned_disciplines": disciplines,
        "assigned_disciplines_text": ", ".join(disciplines),
        "assigned_disciplines_count": len(disciplines),
    }


def _resolve_section_teacher_in_session(session, course_id, lesson_type, payload):
    teacher_id = payload.get("teacher_id")
    teacher_name = payload.get("teacher_name", "")

    if teacher_id:
        teacher = session.get(Teacher, int(teacher_id))
        if teacher:
            return teacher.id, teacher.name

    component_teacher = session.execute(
        select(
            CourseComponent.teacher_id.label("teacher_id"),
            CourseComponent.teacher_name.label("teacher_name"),
        )
        .where(
            CourseComponent.course_id == course_id,
            CourseComponent.lesson_type == lesson_type,
            CourseComponent.teacher_id.is_not(None),
        )
        .order_by(CourseComponent.academic_period, CourseComponent.id)
        .limit(1)
    ).mappings().first()
    if component_teacher:
        return component_teacher["teacher_id"], component_teacher.get("teacher_name", "")

    course_teacher = session.execute(
        select(
            Course.instructor_id.label("instructor_id"),
            Course.instructor_name.label("instructor_name"),
        ).where(Course.id == course_id)
    ).mappings().first()
    if course_teacher:
        return course_teacher.get("instructor_id"), course_teacher.get("instructor_name", "")

    return None, teacher_name


def resolve_section_teacher(connection, course_id, lesson_type, payload):
    with SessionLocal() as session:
        return _resolve_section_teacher_in_session(
            session,
            course_id,
            lesson_type,
            payload,
        )


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
    with SessionLocal() as session:
        all_groups = [
            dict(row)
            for row in session.execute(
                select(
                    Group.id.label("id"),
                    Group.name.label("name"),
                    Group.programme.label("programme"),
                    Group.specialty_code.label("specialty_code"),
                    Group.study_course.label("study_course"),
                )
                .where(
                    Group.programme.is_not(None),
                    Group.programme != "",
                    Group.study_course.is_not(None),
                )
                .order_by(Group.study_course, Group.programme, Group.name)
            ).mappings().all()
        ]
        groups = [
            group
            for group in all_groups
            if (not study_course or int(group.get("study_course") or 0) == study_course)
            and (
                not programme
                or _same_education_group(group.get("programme"), group.get("specialty_code"), programme, "")
            )
        ]

        component_filters = [CourseComponent.lesson_type.in_(("lecture", "practical", "lab"))]
        if semester:
            component_filters.append(CourseComponent.academic_period == semester)
        if study_course:
            component_filters.append(Course.year == study_course)
        components = [
            dict(row)
            for row in session.execute(
                select(
                    CourseComponent.course_id.label("course_id"),
                    CourseComponent.course_name.label("course_name"),
                    CourseComponent.lesson_type.label("lesson_type"),
                    CourseComponent.weekly_classes.label("weekly_classes"),
                    CourseComponent.requires_computers.label("requires_computers"),
                    Course.programme.label("programme"),
                    Course.year.label("year"),
                    Course.semester.label("semester"),
                )
                .join(Course, Course.id == CourseComponent.course_id)
                .where(*component_filters)
                .order_by(Course.year, Course.programme, Course.name, CourseComponent.lesson_type)
            ).mappings().all()
            if (not programme or _same_education_group(row.get("programme"), "", programme, ""))
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
                requires_computers = 1 if (
                    component.get("requires_computers")
                    or section_requires_computers(
                        lesson_type,
                        "",
                        component.get("course_name"),
                        component.get("year"),
                    )
                ) else 0
                teacher_id, teacher_name = _resolve_section_teacher_in_session(
                    session,
                    component["course_id"],
                    lesson_type,
                    {},
                )
                existing = session.scalar(
                    select(Section)
                    .where(
                        Section.course_id == component["course_id"],
                        Section.group_id == group["id"],
                        Section.lesson_type == lesson_type,
                    )
                    .limit(1)
                )

                if existing:
                    existing.course_id = component["course_id"]
                    existing.course_name = component["course_name"]
                    existing.group_id = group["id"]
                    existing.group_name = group["name"]
                    existing.classes_count = classes_count
                    existing.lesson_type = lesson_type
                    existing.subgroup_mode = subgroup_mode
                    existing.subgroup_count = subgroup_count
                    existing.requires_computers = requires_computers
                    existing.teacher_id = teacher_id
                    existing.teacher_name = teacher_name or ""
                    section_id = existing.id
                    updated += 1
                else:
                    row = Section(
                        course_id=component["course_id"],
                        course_name=component["course_name"],
                        group_id=group["id"],
                        group_name=group["name"],
                        classes_count=classes_count,
                        lesson_type=lesson_type,
                        subgroup_mode=subgroup_mode,
                        subgroup_count=subgroup_count,
                        requires_computers=requires_computers,
                        teacher_id=teacher_id,
                        teacher_name=teacher_name or "",
                    )
                    session.add(row)
                    session.flush()
                    section_id = row.id
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

        session.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": 0,
        "missing": {"groups": False, "components": False},
        "sections": generated_sections,
    }


def _room_blocked_slots_from_session(session, semester=None, year=None):
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
        day = normalize_room_block_day(row.get("day"))
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


def _find_alternative_room_for_schedule(session, schedule_row, blocked_slots_by_room=None, excluded_room_ids=None):
    blocked_slots_by_room = blocked_slots_by_room or {}
    excluded_room_ids = set(excluded_room_ids or [])
    rooms = [
        dict(row)
        for row in session.execute(
            select(
                Room.id.label("id"),
                Room.number.label("number"),
                Room.capacity.label("capacity"),
                Room.type.label("type"),
                Room.available.label("available"),
                Room.computer_count.label("computer_count"),
                Room.programme.label("programme"),
            )
            .where(func.coalesce(Room.available, 1) == 1)
            .order_by(Room.id)
        ).mappings().all()
    ]
    normalized_day = normalize_room_block_day(schedule_row.get("day"))
    lesson_type = normalize_lesson_type(schedule_row.get("lesson_type"))
    requires_computers = bool(schedule_row.get("requires_computers")) or lesson_type == "lab"
    candidates = []
    for room in rooms:
        if room["id"] == schedule_row.get("room_id") or room["id"] in excluded_room_ids:
            continue
        if is_physical_education_room(room.get("number")) and not is_physical_education_course(
            schedule_row.get("course_name"),
            schedule_row.get("course_code"),
        ):
            continue
        if not schedule_room_type_matches(room.get("type"), lesson_type, requires_computers):
            continue
        if int(room.get("capacity") or 0) < int(schedule_row.get("effective_student_count") or 0):
            continue
        if requires_computers and int(room.get("computer_count") or 0) < MIN_COMPUTER_COUNT:
            continue
        if session.scalar(
            select(Schedule.id)
            .where(
                Schedule.room_id == room["id"],
                Schedule.day == schedule_row.get("day"),
                Schedule.start_hour == schedule_row.get("start_hour"),
                Schedule.id != schedule_row["id"],
            )
            .limit(1)
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
    relocated = []
    notification_pairs = []
    with SessionLocal() as session:
        blocked_slots_by_room = _room_blocked_slots_from_session(
            session,
            room_block.get("semester"),
            room_block.get("year"),
        )
        schedule_rows = [
            dict(row)
            for row in session.execute(
                select(
                    Schedule.id.label("id"),
                    Schedule.section_id.label("section_id"),
                    Schedule.course_id.label("course_id"),
                    Schedule.course_name.label("course_name"),
                    Schedule.teacher_id.label("teacher_id"),
                    Schedule.teacher_name.label("teacher_name"),
                    Schedule.room_id.label("room_id"),
                    Schedule.room_number.label("room_number"),
                    Schedule.room_programme.label("room_programme"),
                    Schedule.room_programme_mismatch.label("room_programme_mismatch"),
                    Schedule.day.label("day"),
                    Schedule.start_hour.label("start_hour"),
                    Schedule.semester.label("semester"),
                    Schedule.year.label("year"),
                    Schedule.group_id.label("group_id"),
                    Schedule.group_name.label("group_name"),
                    Schedule.subgroup.label("subgroup"),
                    Section.lesson_type.label("lesson_type"),
                    Section.subgroup_count.label("subgroup_count"),
                    Section.requires_computers.label("requires_computers"),
                    Course.code.label("course_code"),
                    Group.student_count.label("student_count"),
                    Group.has_subgroups.label("has_subgroups"),
                    Group.programme.label("group_programme"),
                    Group.specialty_code.label("specialty_code"),
                )
                .join(Section, Section.id == Schedule.section_id)
                .join(Course, Course.id == Schedule.course_id)
                .join(Group, Group.id == Schedule.group_id)
                .where(Schedule.room_id == room_block.get("room_id"))
            ).mappings().all()
        ]
        conflicting_schedules = [
            row
            for row in schedule_rows
            if (room_block.get("semester") in (None, "") or row.get("semester") == room_block.get("semester"))
            and (room_block.get("year") in (None, "") or row.get("year") == room_block.get("year"))
            and normalize_room_block_day(row.get("day")) == normalize_room_block_day(room_block.get("day"))
            and int(room_block.get("start_hour") or 0) <= int(row.get("start_hour") or 0) < int(room_block.get("end_hour") or 0)
        ]
        for schedule_row in conflicting_schedules:
            schedule_row["effective_student_count"] = schedule_student_count_for_room(
                schedule_row,
                schedule_row,
                normalize_subgroup(schedule_row.get("subgroup")),
            )
            alternative_room = _find_alternative_room_for_schedule(
                session,
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
            room_programme, room_programme_mismatch = _resolve_schedule_room_programme_meta_in_session(
                session,
                schedule_row["section_id"],
                alternative_room["id"],
            )
            row = session.get(Schedule, schedule_row["id"])
            row.room_id = alternative_room["id"]
            row.room_number = alternative_room.get("number", "")
            row.room_programme = room_programme
            row.room_programme_mismatch = room_programme_mismatch
            row.relocated_from_room_number = schedule_row.get("room_number", "")
            row.relocation_reason = room_block.get("reason", "") or "room_block"
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
            notification_pairs.append((before_schedule, after_schedule))
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
        session.commit()
    for before_schedule, after_schedule in notification_pairs:
        create_schedule_change_notifications(connection, before_item=before_schedule, after_item=after_schedule)
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

    with SessionLocal() as session:
        section = session.execute(
            select(
                Section.id.label("id"),
                Section.course_id.label("course_id"),
                Section.course_name.label("course_name"),
                Section.group_id.label("group_id"),
                Section.group_name.label("group_name"),
                Section.lesson_type.label("lesson_type"),
                Section.subgroup_mode.label("subgroup_mode"),
                Section.subgroup_count.label("subgroup_count"),
                Section.requires_computers.label("requires_computers"),
                func.coalesce(Section.teacher_id, Course.instructor_id).label("teacher_id"),
                func.coalesce(func.nullif(Section.teacher_name, ""), Course.instructor_name, "").label("teacher_name"),
                Course.code.label("course_code"),
                Course.year.label("course_year"),
                Course.programme.label("course_programme"),
                Group.student_count.label("student_count"),
                Group.has_subgroups.label("has_subgroups"),
                Group.study_course.label("study_course"),
            )
            .join(Course, Course.id == Section.course_id)
            .join(Group, Group.id == Section.group_id)
            .where(Section.id == section_id)
        ).mappings().first()
        if section is None:
            raise ApiError(400, "bad_request", "Для расписания не найдена секция")

        room = session.execute(
            select(
                Room.id.label("id"),
                Room.number.label("number"),
                Room.capacity.label("capacity"),
                Room.type.label("type"),
                Room.available.label("available"),
                Room.computer_count.label("computer_count"),
                Room.programme.label("programme"),
            ).where(Room.id == room_id)
        ).mappings().first()
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
        if is_physical_education_room(room.get("number")) and not is_physical_education_course(
            section.get("course_name"),
            section.get("course_code"),
        ):
            raise ApiError(400, "bad_request", "В аудитории Орленок можно проводить только физкультуру")

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

        room_conflict = select(Schedule.id).where(
            Schedule.room_id == room_id,
            Schedule.day == day,
            Schedule.start_hour == start_hour,
        )
        teacher_conflict = select(Schedule.id).where(
            Schedule.teacher_id == teacher_id,
            Schedule.day == day,
            Schedule.start_hour == start_hour,
        )
        group_conflict = select(Schedule.id).where(
            Schedule.group_id == section["group_id"],
            Schedule.day == day,
            Schedule.start_hour == start_hour,
            or_(
                func.coalesce(Schedule.subgroup, "") == "",
                subgroup == "",
                func.upper(Schedule.subgroup) == subgroup,
            ),
        )
        if exclude_schedule_id is not None:
            room_conflict = room_conflict.where(Schedule.id != exclude_schedule_id)
            teacher_conflict = teacher_conflict.where(Schedule.id != exclude_schedule_id)
            group_conflict = group_conflict.where(Schedule.id != exclude_schedule_id)

        if session.scalar(room_conflict.limit(1)):
            raise ApiError(400, "bad_request", "Аудитория уже занята в это время")

        room_blocked_slots = _room_blocked_slots_from_session(
            session,
            payload.get("semester"),
            payload.get("year"),
        )
        if (normalize_room_block_day(day), int(start_hour)) in room_blocked_slots.get(room_id, set()):
            raise ApiError(400, "bad_request", "Аудитория недоступна в этот временной слот")

        if session.scalar(teacher_conflict.limit(1)):
            raise ApiError(400, "bad_request", "Преподаватель уже занят в это время")

        if session.scalar(group_conflict.limit(1)):
            raise ApiError(400, "bad_request", "Группа или подгруппа уже занята в это время")


def _generated_subgroups_by_group(session):
    rows = session.execute(
        select(Schedule.group_id, Schedule.subgroup)
        .where(Schedule.subgroup.is_not(None), Schedule.subgroup != "")
    ).all()
    subgroups = {}
    for group_id, subgroup in rows:
        value = str(subgroup or "").strip().upper()
        if value:
            subgroups.setdefault(group_id, set()).add(value)
    return subgroups


def _course_to_dict(row):
    return {
        "id": row.id,
        "name": row.name,
        "code": row.code,
        "credits": row.credits,
        "hours": row.hours,
        "description": row.description,
        "year": row.year,
        "semester": row.semester,
        "department": row.department,
        "instructor_id": row.instructor_id,
        "instructor_name": row.instructor_name,
        "programme": row.programme,
        "module_type": row.module_type,
        "module_name": row.module_name,
        "cycle": row.cycle,
        "component": row.component,
        "language": row.language,
        "academic_year": row.academic_year,
        "entry_year": row.entry_year,
        "requires_computers": row.requires_computers,
    }


def _course_component_to_dict(row):
    return {
        "id": row.id,
        "course_id": row.course_id,
        "course_code": row.course_code,
        "course_name": row.course_name,
        "programme": row.programme,
        "study_year": row.study_year,
        "academic_period": row.academic_period,
        "semester": row.semester,
        "lesson_type": row.lesson_type,
        "hours": row.hours,
        "weekly_classes": row.weekly_classes,
        "requires_computers": row.requires_computers,
        "teacher_id": row.teacher_id,
        "teacher_name": row.teacher_name,
    }


def _is_orlenok_room(room_number):
    return "орленок" in str(room_number or "").strip().lower()


def _room_building_value(payload):
    if _is_orlenok_room(payload.get("number")):
        return ""
    return str(payload.get("building", "") or "")


def _optional_int_filter(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _room_to_dict(row):
    return {
        "id": row.id,
        "number": row.number,
        "capacity": row.capacity,
        "building": "" if _is_orlenok_room(row.number) else row.building,
        "type": row.type,
        "equipment": row.equipment,
        "programme": row.programme,
        "available": row.available,
        "computer_count": row.computer_count,
    }


def _room_block_to_dict(row):
    return {
        "id": row.id,
        "room_id": row.room_id,
        "day": row.day,
        "start_hour": row.start_hour,
        "end_hour": row.end_hour,
        "semester": row.semester,
        "year": row.year,
        "reason": row.reason,
    }


def _group_to_dict(row):
    return {
        "id": row.id,
        "name": row.name,
        "student_count": row.student_count,
        "has_subgroups": row.has_subgroups,
        "language": row.language,
        "programme": row.programme,
        "specialty_code": row.specialty_code,
        "entry_year": row.entry_year,
        "study_course": row.study_course,
    }


def _section_to_dict(row):
    return {
        "id": row.id,
        "course_id": row.course_id,
        "course_name": row.course_name,
        "group_id": row.group_id,
        "group_name": row.group_name,
        "classes_count": row.classes_count,
        "lesson_type": row.lesson_type,
        "subgroup_mode": row.subgroup_mode,
        "subgroup_count": row.subgroup_count,
        "requires_computers": row.requires_computers,
        "teacher_id": row.teacher_id,
        "teacher_name": row.teacher_name,
        "iup_entry_id": row.iup_entry_id,
        "source": row.source,
        "match_method": row.match_method,
    }


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


def get_collection_item(collection, item_id):
    model_map = {
        "courses": Course,
        "course_components": CourseComponent,
        "groups": Group,
        "iup_entries": IupEntry,
        "rooms": Room,
        "room_blocks": RoomBlock,
        "schedules": Schedule,
        "sections": Section,
        "students": Student,
        "teachers": Teacher,
        "users": User,
    }
    model = model_map.get(collection)
    if model is None:
        raise ApiError(400, "unsupported_collection", "Unsupported collection")

    with SessionLocal() as session:
        row = session.get(model, item_id)
        if row is None:
            return None
        if collection == "courses":
            return _course_to_dict(row)
        if collection == "course_components":
            return _course_component_to_dict(row)
        if collection == "groups":
            return _group_to_dict(row)
        if collection == "rooms":
            return _room_to_dict(row)
        if collection == "room_blocks":
            return _room_block_to_dict(row)
        if collection == "schedules":
            return _schedule_to_dict(row)
        if collection == "sections":
            return _section_to_dict(row)
        if collection == "teachers":
            return {
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "phone": row.phone,
                "department": row.department,
                "subject_taught": row.subject_taught,
                "weekly_hours_limit": row.weekly_hours_limit,
                "teaching_languages": row.teaching_languages,
            }
        if collection == "students":
            return {
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "department": row.department,
                "programme": row.programme,
                "group_id": row.group_id,
                "group_name": row.group_name,
                "subgroup": row.subgroup,
                "language": row.language,
            }
        if collection == "users":
            return {
                "id": row.id,
                "email": row.email,
                "full_name": row.full_name,
                "role": row.role,
                "token": row.token,
            }
        return {
            column.name: getattr(row, column.name)
            for column in row.__table__.columns
        }


def list_collection(connection, collection, query, user=None):
    if collection == "users":
        with SessionLocal() as session:
            return [
                {
                    "id": row.id,
                    "email": row.email,
                    "displayName": row.full_name,
                    "role": row.role,
                    "token": row.token,
                }
                for row in session.scalars(select(User).order_by(User.id)).all()
            ]

    if collection == "courses":
        with SessionLocal() as session:
            return [_course_to_dict(row) for row in session.scalars(select(Course).order_by(Course.id)).all()]

    if collection == "course_components":
        course_id = query.get("course_id", [None])[0]
        academic_period = query.get("academic_period", [None])[0]
        statement = select(CourseComponent)
        if course_id is not None:
            statement = statement.where(CourseComponent.course_id == course_id)
        if academic_period is not None:
            statement = statement.where(CourseComponent.academic_period == academic_period)
        statement = statement.order_by(
            CourseComponent.academic_period,
            CourseComponent.course_name,
            CourseComponent.lesson_type,
            CourseComponent.id,
        )
        with SessionLocal() as session:
            return [_course_component_to_dict(row) for row in session.scalars(statement).all()]

    if collection == "iup_entries":
        with SessionLocal() as session:
            return [
                {
                    "id": row.id,
                    "file_name": row.file_name,
                    "group_name": row.group_name,
                    "programme": row.programme,
                    "study_course": row.study_course,
                    "language": row.language,
                    "academic_year": row.academic_year,
                    "academic_period": row.academic_period,
                    "semester": row.semester,
                    "component": row.component,
                    "course_code": row.course_code,
                    "course_name": row.course_name,
                    "credits": row.credits,
                    "lesson_type": row.lesson_type,
                    "teacher_id": row.teacher_id,
                    "teacher_name": row.teacher_name,
                    "hours": row.hours,
                }
                for row in session.scalars(
                    select(IupEntry).order_by(
                        IupEntry.file_name,
                        IupEntry.academic_period,
                        IupEntry.course_name,
                        IupEntry.lesson_type,
                        IupEntry.id,
                    )
                ).all()
            ]

    if collection == "teachers":
        with SessionLocal() as session:
            teachers = session.execute(
                select(
                    Teacher.id.label("id"),
                    Teacher.name.label("name"),
                    Teacher.email.label("email"),
                    Teacher.phone.label("phone"),
                    Teacher.department.label("department"),
                    Teacher.subject_taught.label("subject_taught"),
                    Teacher.weekly_hours_limit.label("weekly_hours_limit"),
                    Teacher.teaching_languages.label("teaching_languages"),
                ).order_by(Teacher.id)
            ).mappings().all()
        disciplines_by_teacher = _teacher_disciplines_map(connection)
        return [_serialize_teacher(row, disciplines_by_teacher) for row in teachers]

    if collection == "students":
        with SessionLocal() as session:
            return [
                {
                    "id": row.id,
                    "name": row.name,
                    "email": row.email,
                    "department": row.department,
                    "programme": row.programme,
                    "group_id": row.group_id,
                    "group_name": row.group_name,
                    "subgroup": row.subgroup,
                    "language": row.language,
                }
                for row in session.scalars(select(Student).order_by(Student.id)).all()
            ]

    if collection == "rooms":
        with SessionLocal() as session:
            return [_room_to_dict(row) for row in session.scalars(select(Room).order_by(Room.id)).all()]

    if collection == "room_blocks":
        with SessionLocal() as session:
            return [
                _room_block_to_dict(row)
                for row in session.scalars(
                    select(RoomBlock).order_by(
                        RoomBlock.room_id,
                        RoomBlock.day,
                        RoomBlock.start_hour,
                        RoomBlock.id,
                    )
                ).all()
            ]

    if collection == "groups":
        with SessionLocal() as session:
            groups = session.scalars(select(Group).order_by(Group.id)).all()
            subgroups_by_group = _generated_subgroups_by_group(session)
            return [
                {
                    **_group_to_dict(row),
                    "auto_has_subgroups": 1 if subgroups_by_group.get(row.id) else 0,
                    "generated_subgroups": ",".join(sorted(subgroups_by_group.get(row.id, set()))),
                }
                for row in groups
            ]

    if collection == "sections":
        with SessionLocal() as session:
            return [_section_to_dict(row) for row in session.scalars(select(Section).order_by(Section.id)).all()]

    clauses = []
    semester = _optional_int_filter(query.get("semester", [None])[0])
    year = _optional_int_filter(query.get("year", [None])[0])
    statement = (
        select(
            Schedule.id.label("id"),
            Schedule.section_id.label("section_id"),
            Schedule.course_id.label("course_id"),
            Schedule.course_name.label("course_name"),
            Schedule.teacher_id.label("teacher_id"),
            Schedule.teacher_name.label("teacher_name"),
            Schedule.room_id.label("room_id"),
            Schedule.room_number.label("room_number"),
            Schedule.group_id.label("group_id"),
            Schedule.group_name.label("group_name"),
            Schedule.subgroup.label("subgroup"),
            Schedule.day.label("day"),
            Schedule.start_hour.label("start_hour"),
            Schedule.semester.label("semester"),
            Schedule.year.label("year"),
            Schedule.algorithm.label("algorithm"),
            func.coalesce(Section.lesson_type, "lecture").label("lesson_type"),
            Schedule.room_programme.label("room_programme"),
            Schedule.room_programme_mismatch.label("room_programme_mismatch"),
            Schedule.relocated_from_room_number.label("relocated_from_room_number"),
            Schedule.relocation_reason.label("relocation_reason"),
        )
        .select_from(Schedule)
        .outerjoin(Section, Section.id == Schedule.section_id)
    )
    if semester is not None:
        statement = statement.where(Schedule.semester == semester)
    if year is not None:
        statement = statement.where(Schedule.year == year)
    if collection == "schedules" and user and user.get("role") == "student":
        if not user.get("group_id"):
            return []
        statement = statement.where(Schedule.group_id == user["group_id"])
        if user.get("subgroup") in {"A", "B"}:
            statement = statement.where(
                (func.coalesce(Schedule.subgroup, "") == "")
                | (func.upper(Schedule.subgroup) == user["subgroup"])
            )
        else:
            statement = statement.where(func.coalesce(Schedule.subgroup, "") == "")
    elif collection == "schedules" and user and user.get("role") == "teacher":
        statement = statement.outerjoin(Teacher, Teacher.id == Schedule.teacher_id)
        statement = statement.where(
            (func.lower(func.coalesce(Teacher.email, "")) == str(user.get("email", "")).lower())
            | (
                func.lower(func.coalesce(Schedule.teacher_name, ""))
                == str(user.get("full_name", "")).lower()
            )
        )
    statement = statement.order_by(Schedule.day, Schedule.start_hour, Schedule.id)
    with SessionLocal() as session:
        return [dict(row) for row in session.execute(statement).mappings().all()]


def create_collection_item(connection, collection, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours", "year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        with SessionLocal() as session:
            row = Course(
                name=course_name,
                code=course_code,
                credits=normalized.get("credits"),
                hours=normalized.get("hours"),
                description=normalized.get("description", ""),
                year=normalized.get("year", normalized.get("study_year")),
                semester=normalized.get("semester"),
                department=normalized.get("department", ""),
                instructor_id=normalized.get("instructor_id"),
                instructor_name=normalized.get("instructor_name", ""),
                programme=normalized.get("programme", normalized.get("programme_name", "")),
                module_type=normalized.get("module_type", ""),
                module_name=normalized.get("module_name", ""),
                cycle=normalized.get("cycle", ""),
                component=normalized.get("component", ""),
                language=normalized.get("language", ""),
                academic_year=normalized.get("academic_year", ""),
                entry_year=normalized.get("entry_year", ""),
                requires_computers=1 if normalized.get("requires_computers", 0) else 0,
            )
            session.add(row)
            session.commit()
            return _course_to_dict(row)

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
        with SessionLocal() as session:
            row = CourseComponent(
                course_id=normalized.get("course_id"),
                course_code=normalized.get("course_code", ""),
                course_name=normalized.get("course_name", ""),
                programme=normalized.get("programme", ""),
                study_year=normalized.get("study_year"),
                academic_period=normalized.get("academic_period"),
                semester=normalized.get("semester"),
                lesson_type=lesson_type,
                hours=normalized.get("hours"),
                weekly_classes=normalized.get("weekly_classes"),
                requires_computers=section_requires_computers(
                    lesson_type,
                    normalized.get("course_code", ""),
                    normalized.get("course_name", ""),
                    normalized.get("study_year"),
                ),
                teacher_id=normalized.get("teacher_id"),
                teacher_name=normalized.get("teacher_name", ""),
            )
            session.add(row)
            session.commit()
            return _course_component_to_dict(row)

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["weekly_hours_limit", "max_hours_per_week"])
        validate_teacher_email(normalized.get("email"))
        teaching_languages = ",".join(normalize_teaching_languages(normalized.get("teaching_languages")))
        with SessionLocal() as session:
            row = Teacher(
                name=normalized.get("name"),
                email=normalized.get("email"),
                phone=normalized.get("phone", ""),
                department=normalized.get("department", ""),
                subject_taught=normalized.get("subject_taught", normalized.get("specialization", "")),
                weekly_hours_limit=normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages=teaching_languages,
                name_normalized=normalize_teacher_name(normalized.get("name")),
                name_signature=build_teacher_name_signature(normalized.get("name")),
            )
            session.add(row)
            session.commit()
            teacher = {
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "phone": row.phone,
                "department": row.department,
                "subject_taught": row.subject_taught,
                "weekly_hours_limit": row.weekly_hours_limit,
                "teaching_languages": row.teaching_languages,
            }
        return _serialize_teacher(teacher, _teacher_disciplines_map(connection))

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        with SessionLocal() as session:
            row = Room(
                number=normalized.get("number"),
                capacity=normalized.get("capacity"),
                building=_room_building_value(normalized),
                type=normalized.get("type", ""),
                equipment=normalized.get("equipment", ""),
                programme=normalized.get("programme", normalized.get("department", "")),
                available=1 if normalized.get("available", normalized.get("is_available", 1)) else 0,
                computer_count=normalized.get("computer_count", 0),
            )
            session.add(row)
            session.commit()
            return _room_to_dict(row)

    if collection == "room_blocks":
        normalized = normalize_room_block_interval(payload)
        relocated = _relocate_conflicting_room_schedules(connection, normalized)
        with SessionLocal() as session:
            row = RoomBlock(
                room_id=normalized.get("room_id"),
                day=normalized.get("day"),
                start_hour=normalized.get("start_hour"),
                end_hour=normalized.get("end_hour"),
                semester=normalized.get("semester"),
                year=normalized.get("year"),
                reason=normalized.get("reason", ""),
            )
            session.add(row)
            session.commit()
            return {**_room_block_to_dict(row), "relocatedSchedules": relocated}

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
        with SessionLocal() as session:
            row = Group(
                name=normalized.get("name"),
                student_count=normalized.get("student_count") or 0,
                has_subgroups=1 if normalized.get("has_subgroups", 0) else 0,
                language=group_language,
                programme=programme,
                specialty_code=specialty_code,
                entry_year=entry_year,
                study_course=study_course,
            )
            session.add(row)
            session.commit()
            return _group_to_dict(row)

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
        with SessionLocal() as session:
            row = Section(
                course_id=normalized.get("course_id"),
                course_name=normalized.get("course_name"),
                group_id=normalized.get("group_id"),
                group_name=normalized.get("group_name", ""),
                classes_count=normalized.get("classes_count", normalized.get("class_count")),
                lesson_type=normalized["lesson_type"],
                subgroup_mode=normalized["subgroup_mode"],
                subgroup_count=normalized["subgroup_count"],
                requires_computers=requires_computers,
                teacher_id=teacher_id,
                teacher_name=teacher_name,
                iup_entry_id=normalized.get("iup_entry_id"),
                source=normalized.get("source", "manual"),
                match_method=normalized.get("match_method", "manual"),
            )
            session.add(row)
            session.commit()
            return _section_to_dict(row)

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
        with SessionLocal() as session:
            row = Schedule(
                section_id=normalized.get("section_id"),
                course_id=normalized.get("course_id"),
                course_name=normalized.get("course_name"),
                teacher_id=normalized.get("teacher_id"),
                teacher_name=normalized.get("teacher_name"),
                room_id=normalized.get("room_id"),
                room_number=normalized.get("room_number"),
                group_id=normalized.get("group_id"),
                group_name=normalized.get("group_name"),
                subgroup=normalized.get("subgroup", ""),
                day=normalized.get("day"),
                start_hour=normalized.get("start_hour"),
                semester=normalized.get("semester"),
                year=normalized.get("year"),
                algorithm=normalized.get("algorithm"),
                room_programme=room_programme,
                room_programme_mismatch=room_programme_mismatch,
                relocated_from_room_number=normalized.get("relocated_from_room_number", ""),
                relocation_reason=normalized.get("relocation_reason", ""),
            )
            session.add(row)
            session.commit()
            result = _schedule_to_dict(row)
        recompute_room_availability(connection)
        return result

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def _resolve_schedule_room_programme_meta_in_session(session, section_id, room_id):
    row = session.execute(
        select(
            Course.programme.label("course_programme"),
            Group.programme.label("group_programme"),
            Group.specialty_code.label("specialty_code"),
            Room.programme.label("room_programme"),
        )
        .select_from(Section)
        .join(Course, Course.id == Section.course_id)
        .join(Group, Group.id == Section.group_id)
        .join(Room, Room.id == room_id)
        .where(Section.id == section_id)
    ).mappings().first()
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


def resolve_schedule_room_programme_meta(connection, section_id, room_id):
    with SessionLocal() as session:
        return _resolve_schedule_room_programme_meta_in_session(session, section_id, room_id)


def update_collection_item(connection, collection, item_id, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours", "year", "study_year", "semester", "instructor_id", "requires_computers"])
        course_name = normalized.get("name")
        course_code = normalized.get("code") or course_name
        with SessionLocal() as session:
            row = session.get(Course, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.name = course_name
            row.code = course_code
            row.credits = normalized.get("credits")
            row.hours = normalized.get("hours")
            row.description = normalized.get("description", "")
            row.year = normalized.get("year", normalized.get("study_year"))
            row.semester = normalized.get("semester")
            row.department = normalized.get("department", "")
            row.instructor_id = normalized.get("instructor_id")
            row.instructor_name = normalized.get("instructor_name", "")
            row.programme = normalized.get("programme", normalized.get("programme_name", ""))
            row.module_type = normalized.get("module_type", "")
            row.module_name = normalized.get("module_name", "")
            row.cycle = normalized.get("cycle", "")
            row.component = normalized.get("component", "")
            row.language = normalized.get("language", "")
            row.academic_year = normalized.get("academic_year", "")
            row.entry_year = normalized.get("entry_year", "")
            row.requires_computers = 1 if normalized.get("requires_computers", 0) else 0
            session.commit()
            result = _course_to_dict(row)
        recompute_room_availability(connection)
        return result

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["weekly_hours_limit", "max_hours_per_week"])
        validate_teacher_email(normalized.get("email"))
        teaching_languages = ",".join(normalize_teaching_languages(normalized.get("teaching_languages")))
        with SessionLocal() as session:
            row = session.get(Teacher, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.name = normalized.get("name")
            row.email = normalized.get("email")
            row.phone = normalized.get("phone", "")
            row.department = normalized.get("department", "")
            row.subject_taught = normalized.get("subject_taught", normalized.get("specialization", ""))
            row.weekly_hours_limit = normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week"))
            row.teaching_languages = teaching_languages
            row.name_normalized = normalize_teacher_name(normalized.get("name"))
            row.name_signature = build_teacher_name_signature(normalized.get("name"))
            session.commit()
            teacher = {
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "phone": row.phone,
                "department": row.department,
                "subject_taught": row.subject_taught,
                "weekly_hours_limit": row.weekly_hours_limit,
                "teaching_languages": row.teaching_languages,
            }
        return _serialize_teacher(teacher, _teacher_disciplines_map(connection))

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity", "available", "is_available", "computer_count"])
        with SessionLocal() as session:
            row = session.get(Room, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.number = normalized.get("number")
            row.capacity = normalized.get("capacity")
            row.building = _room_building_value(normalized)
            row.type = normalized.get("type", "")
            row.equipment = normalized.get("equipment", "")
            row.programme = normalized.get("programme", normalized.get("department", ""))
            row.available = 1 if normalized.get("available", normalized.get("is_available", 1)) else 0
            row.computer_count = normalized.get("computer_count", 0)
            session.commit()
            return _room_to_dict(row)

    if collection == "room_blocks":
        normalized = normalize_room_block_interval(payload)
        relocated = _relocate_conflicting_room_schedules(connection, normalized, exclude_block_id=item_id)
        with SessionLocal() as session:
            row = session.get(RoomBlock, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.room_id = normalized.get("room_id")
            row.day = normalized.get("day")
            row.start_hour = normalized.get("start_hour")
            row.end_hour = normalized.get("end_hour")
            row.semester = normalized.get("semester")
            row.year = normalized.get("year")
            row.reason = normalized.get("reason", "")
            session.commit()
            return {**_room_block_to_dict(row), "relocatedSchedules": relocated}

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
        with SessionLocal() as session:
            row = session.get(Group, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.name = normalized.get("name")
            row.student_count = normalized.get("student_count") or 0
            row.has_subgroups = 1 if normalized.get("has_subgroups", 0) else 0
            row.language = group_language
            row.programme = programme
            row.specialty_code = specialty_code
            row.entry_year = entry_year
            row.study_course = study_course
            session.commit()
            return _group_to_dict(row)

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
        with SessionLocal() as session:
            row = session.get(Section, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.course_id = normalized.get("course_id")
            row.course_name = normalized.get("course_name")
            row.group_id = normalized.get("group_id")
            row.group_name = normalized.get("group_name", "")
            row.classes_count = normalized.get("classes_count", normalized.get("class_count"))
            row.lesson_type = normalized["lesson_type"]
            row.subgroup_mode = normalized["subgroup_mode"]
            row.subgroup_count = normalized["subgroup_count"]
            row.requires_computers = requires_computers
            row.teacher_id = teacher_id
            row.teacher_name = teacher_name
            row.iup_entry_id = normalized.get("iup_entry_id")
            row.source = normalized.get("source", "manual")
            row.match_method = normalized.get("match_method", "manual")
            session.commit()
            return _section_to_dict(row)

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
        with SessionLocal() as session:
            row = session.get(Schedule, item_id)
            if row is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            row.section_id = normalized.get("section_id")
            row.course_id = normalized.get("course_id")
            row.course_name = normalized.get("course_name")
            row.teacher_id = normalized.get("teacher_id")
            row.teacher_name = normalized.get("teacher_name")
            row.room_id = normalized.get("room_id")
            row.room_number = normalized.get("room_number")
            row.group_id = normalized.get("group_id")
            row.group_name = normalized.get("group_name")
            row.subgroup = normalized.get("subgroup", "")
            row.day = normalized.get("day")
            row.start_hour = normalized.get("start_hour")
            row.semester = normalized.get("semester")
            row.year = normalized.get("year")
            row.algorithm = normalized.get("algorithm")
            row.room_programme = room_programme
            row.room_programme_mismatch = room_programme_mismatch
            row.relocated_from_room_number = normalized.get("relocated_from_room_number", "")
            row.relocation_reason = normalized.get("relocation_reason", "")
            session.commit()
            return _schedule_to_dict(row)

    raise ApiError(400, "unsupported_collection", "Unsupported collection")


def delete_collection_item(connection, collection, item_id):
    schedules_changed = False
    collection_models = {
        "courses": Course,
        "course_components": CourseComponent,
        "groups": Group,
        "teachers": Teacher,
        "rooms": Room,
        "room_blocks": RoomBlock,
        "students": Student,
        "sections": Section,
        "schedules": Schedule,
    }
    model = collection_models.get(collection)
    if model is None:
        raise ApiError(400, "unsupported_collection", "Unsupported collection")

    with SessionLocal() as session:
        if collection == "courses":
            course = session.get(Course, item_id)
            session.execute(delete(Schedule).where(Schedule.course_id == item_id))
            schedules_changed = True
            session.execute(delete(Section).where(Section.course_id == item_id))
            session.execute(delete(CourseComponent).where(CourseComponent.course_id == item_id))
            if course:
                session.execute(
                    delete(IupEntry).where(
                        func.lower(IupEntry.course_code) == str(course.code or "").lower()
                    )
                )
        elif collection == "groups":
            group = session.get(Group, item_id)
            session.execute(delete(Schedule).where(Schedule.group_id == item_id))
            schedules_changed = True
            session.execute(delete(Section).where(Section.group_id == item_id))
            session.execute(
                update(Student)
                .where(Student.group_id == item_id)
                .values(group_id=None, group_name="", subgroup="")
            )
            if group:
                session.execute(delete(IupEntry).where(IupEntry.group_name == group.name))
        elif collection == "teachers":
            session.execute(delete(Schedule).where(Schedule.teacher_id == item_id))
            schedules_changed = True
            session.execute(
                update(Course)
                .where(Course.instructor_id == item_id)
                .values(instructor_id=None, instructor_name="")
            )
            session.execute(
                update(CourseComponent)
                .where(CourseComponent.teacher_id == item_id)
                .values(teacher_id=None, teacher_name="")
            )
            session.execute(
                update(Section)
                .where(Section.teacher_id == item_id)
                .values(teacher_id=None, teacher_name="")
            )
            session.execute(
                delete(TeacherPreferenceRequest).where(
                    TeacherPreferenceRequest.teacher_id == item_id
                )
            )
            session.execute(
                delete(Notification).where(
                    Notification.recipient_role == "teacher",
                    Notification.recipient_id == item_id,
                )
            )
        elif collection == "rooms":
            session.execute(delete(Schedule).where(Schedule.room_id == item_id))
            session.execute(delete(RoomBlock).where(RoomBlock.room_id == item_id))
            schedules_changed = True
        elif collection == "students":
            session.execute(
                delete(Notification).where(
                    Notification.recipient_role == "student",
                    Notification.recipient_id == item_id,
                )
            )
        session.execute(delete(model).where(model.id == item_id))
        session.commit()

    if schedules_changed and collection != "rooms":
        recompute_room_availability(connection)
