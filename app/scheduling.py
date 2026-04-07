from datetime import date, timedelta

from .db import db_execute, db_executemany, query_all
from .errors import ApiError


def monday_for_week(target_year):
    today = date.today()
    anchor = date(target_year, today.month, today.day)
    return anchor - timedelta(days=anchor.weekday())


def build_schedule(connection, semester, year, algorithm):
    courses = query_all(
        connection,
        """
        SELECT id, name, instructor_id, instructor_name, semester, year AS study_year
        FROM courses
        ORDER BY id
        """,
    )
    teachers = query_all(connection, "SELECT id, name FROM teachers ORDER BY id")
    rooms = query_all(connection, "SELECT id, number FROM rooms ORDER BY id")

    if not courses or not teachers or not rooms:
        raise ApiError(
            400,
            "schedule_generation_requires_data",
            "Для генерации расписания нужны курсы, преподаватели и аудитории.",
        )

    start_day = monday_for_week(year)
    slots = [(day_idx, hour) for day_idx in range(6) for hour in range(8, 18)]
    generated = []

    for idx, course in enumerate(courses):
        day_idx, hour = slots[idx % len(slots)]
        teacher = next(
            (
                existing_teacher
                for existing_teacher in teachers
                if existing_teacher["id"] == course.get("instructor_id")
            ),
            None,
        ) or teachers[idx % len(teachers)]
        room = rooms[idx % len(rooms)]
        generated.append(
            (
                course["id"],
                course["name"],
                teacher["id"],
                teacher["name"],
                room["id"],
                room["number"],
                (start_day + timedelta(days=day_idx)).isoformat(),
                hour,
                semester,
                year,
                algorithm,
            )
        )

    db_execute(connection, "DELETE FROM schedules")
    db_executemany(
        connection,
        """
        INSERT INTO schedules (
            course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            day, start_hour, semester, year, algorithm
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        generated,
    )
    connection.commit()

    return query_all(
        connection,
        """
        SELECT
            id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            day, start_hour, semester, year, algorithm
        FROM schedules
        ORDER BY day, start_hour, id
        """,
    )
