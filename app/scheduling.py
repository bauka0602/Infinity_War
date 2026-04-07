import math
from datetime import date, timedelta

from .db import db_execute, db_executemany, query_all
from .errors import ApiError


def monday_for_week(target_year):
    today = date.today()
    anchor = date(target_year, today.month, today.day)
    return anchor - timedelta(days=anchor.weekday())


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
            c.instructor_id,
            c.instructor_name,
            c.semester,
            c.year,
            g.student_count,
            g.has_subgroups
        FROM sections s
        JOIN courses c ON c.id = s.course_id
        JOIN groups g ON g.id = s.group_id
        WHERE c.semester = ?
        ORDER BY s.id
        """,
        (semester,),
    )
    teachers = query_all(connection, "SELECT id, name FROM teachers ORDER BY id")
    rooms = query_all(
        connection,
        """
        SELECT id, number, capacity, available
        FROM rooms
        WHERE available = 1
        ORDER BY capacity, id
        """,
    )

    if not sections or not teachers or not rooms:
        raise ApiError(
            400,
            "schedule_generation_requires_data",
            "Для генерации расписания нужны секции, преподаватели и аудитории.",
        )

    start_day = monday_for_week(year)
    slots = [(day_idx, hour) for day_idx in range(5) for hour in range(8, 18)]
    generated = []
    teacher_busy = set()
    room_busy = set()
    group_busy = set()

    def pick_room(required_capacity, day_idx, hour):
        for room in rooms:
            room_capacity = room.get("capacity") or 0
            if room_capacity < required_capacity:
                continue
            if (room["id"], day_idx, hour) in room_busy:
                continue
            return room
        return None

    for section in sections:
        teacher = next(
            (
                existing_teacher
                for existing_teacher in teachers
                if existing_teacher["id"] == section.get("instructor_id")
            ),
            None,
        )
        if teacher is None:
            raise ApiError(
                400,
                "bad_request",
                f"Для курса '{section['course_name']}' не найден преподаватель.",
            )

        subgroup_labels = [""] if not section.get("has_subgroups") else ["A", "B"]
        subgroup_size = (
            math.ceil((section.get("student_count") or 0) / 2)
            if section.get("has_subgroups")
            else section.get("student_count") or 0
        )

        for class_index in range(section.get("classes_count") or 0):
            for subgroup in subgroup_labels:
                placed = False
                for day_idx, hour in slots:
                    teacher_key = (teacher["id"], day_idx, hour)
                    group_key = (section["group_id"], day_idx, hour)
                    if teacher_key in teacher_busy or group_key in group_busy:
                        continue

                    room = pick_room(subgroup_size, day_idx, hour)
                    if room is None:
                        continue

                    teacher_busy.add(teacher_key)
                    group_busy.add(group_key)
                    room_busy.add((room["id"], day_idx, hour))
                    generated.append(
                        (
                            section["id"],
                            section["course_id"],
                            section["course_name"],
                            teacher["id"],
                            teacher["name"],
                            room["id"],
                            room["number"],
                            section["group_id"],
                            section["group_name"],
                            subgroup,
                            (start_day + timedelta(days=day_idx)).isoformat(),
                            hour,
                            semester,
                            year,
                            algorithm,
                        )
                    )
                    placed = True
                    break

                if not placed:
                    subgroup_label = f" ({subgroup})" if subgroup else ""
                    raise ApiError(
                        400,
                        "bad_request",
                        f"Не удалось разместить {section['course_name']} для группы {section['group_name']}{subgroup_label}.",
                    )

    db_execute(connection, "DELETE FROM schedules")
    db_executemany(
        connection,
        """
        INSERT INTO schedules (
            section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        generated,
    )
    connection.commit()

    return query_all(
        connection,
        """
        SELECT
            id, section_id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            group_id, group_name, subgroup, day, start_hour, semester, year, algorithm
        FROM schedules
        ORDER BY day, start_hour, id
        """,
    )
