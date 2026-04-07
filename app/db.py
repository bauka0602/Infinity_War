import json
import sqlite3

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

from .config import DATA_DIR, DATABASE_URL, DB_ENGINE, DB_FILE, LEGACY_JSON_FILE
from .security import hash_password
from .store import default_store


def get_connection():
    if DB_ENGINE == "postgres":
        if psycopg is None:
            raise RuntimeError(
                "psycopg is required when DATABASE_URL points to PostgreSQL."
            )
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_FILE)
    connection.row_factory = sqlite3.Row
    return connection


def sql_query(query):
    if DB_ENGINE == "postgres":
        return query.replace("?", "%s")
    return query


def db_execute(connection, query, params=()):
    return connection.execute(sql_query(query), params)


def db_executemany(connection, query, rows):
    if DB_ENGINE == "postgres":
        with connection.cursor() as cursor:
            cursor.executemany(sql_query(query), rows)
        return
    connection.executemany(query, rows)


def query_all(connection, query, params=()):
    cursor = db_execute(connection, query, params)
    rows = cursor.fetchall()
    return [row if isinstance(row, dict) else dict(row) for row in rows]


def query_one(connection, query, params=()):
    cursor = db_execute(connection, query, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return row if isinstance(row, dict) else dict(row)


def query_scalar(connection, query, params=()):
    cursor = db_execute(connection, query, params)
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def insert_and_get_id(connection, query, params=()):
    if DB_ENGINE == "postgres":
        returning_query = f"{query} RETURNING id"
        return query_scalar(connection, returning_query, params)

    cursor = db_execute(connection, query, params)
    return cursor.lastrowid


def column_exists(connection, table_name, column_name):
    if DB_ENGINE == "postgres":
        return bool(
            query_scalar(
                connection,
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = ? AND column_name = ?
                """,
                (table_name, column_name),
            )
        )

    rows = query_all(connection, f"PRAGMA table_info({table_name})")
    return any(row["name"] == column_name for row in rows)


def ensure_column(connection, table_name, column_name, column_definition):
    if column_exists(connection, table_name, column_name):
        return

    db_execute(
        connection,
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}",
    )


def rename_column(connection, table_name, old_name, new_name):
    if not column_exists(connection, table_name, old_name):
        return
    if column_exists(connection, table_name, new_name):
        return
    db_execute(
        connection,
        f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}",
    )


def seed_from_store(connection, store):
    for user in store["users"]:
        db_execute(
            connection,
            """
            INSERT INTO users (
                email, password, full_name, role, token, avatar_data, department, programme
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user["email"],
                hash_password(user["password"]),
                user["displayName"],
                user["role"],
                user["token"],
                user.get("avatarData"),
                user.get("department", ""),
                user.get("programmeName", ""),
            ),
        )

    for course in store["courses"]:
        db_execute(
            connection,
            """
            INSERT INTO courses (
                name, code, credits, hours, description,
                year, semester, department, instructor_id, instructor_name, programme
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                course["name"],
                course["code"],
                course.get("credits"),
                course.get("hours"),
                course.get("description", ""),
                course.get("study_year"),
                course.get("semester"),
                course.get("department", ""),
                course.get("instructor_id"),
                course.get("instructor_name", ""),
                course.get("programme_name", course.get("programme", "")),
            ),
        )

    for teacher in store["teachers"]:
        db_execute(
            connection,
            """
            INSERT INTO teachers (name, email, phone, department, weekly_hours_limit)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                teacher["name"],
                teacher["email"],
                teacher.get("phone", ""),
                teacher.get("specialization", teacher.get("department", "")),
                teacher.get("max_hours_per_week", teacher.get("weekly_hours_limit")),
            ),
        )

    for room in store["rooms"]:
        db_execute(
            connection,
            """
            INSERT INTO rooms (number, capacity, building, type, equipment, department, available)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                room["number"],
                room["capacity"],
                room.get("building", ""),
                room.get("type", ""),
                room.get("equipment", ""),
                room.get("department", ""),
                room.get("is_available", room.get("available", 1)),
            ),
        )

    for group in store.get("groups", []):
        db_execute(
            connection,
            """
            INSERT INTO groups (name, student_count, has_subgroups)
            VALUES (?, ?, ?)
            """,
            (
                group.get("name"),
                group.get("student_count"),
                group.get("has_subgroups", 0),
            ),
        )

    if store["schedules"]:
        db_executemany(
            connection,
            """
            INSERT INTO schedules (
                course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    schedule.get("course_id"),
                    schedule.get("course_name"),
                    schedule.get("teacher_id"),
                    schedule.get("teacher_name"),
                    schedule.get("room_id"),
                    schedule.get("room_number"),
                    schedule.get("day"),
                    schedule.get("start_hour"),
                    schedule.get("semester"),
                    schedule.get("year"),
                    schedule.get("algorithm"),
                )
                for schedule in store["schedules"]
            ],
        )

    for section in store.get("sections", []):
        db_execute(
            connection,
            """
            INSERT INTO sections (course_id, course_name, group_id, group_name, classes_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                section.get("course_id"),
                section.get("course_name"),
                section.get("group_id"),
                section.get("group_name", ""),
                section.get("class_count", section.get("classes_count")),
            ),
        )


def migrate_legacy_json(connection):
    if not LEGACY_JSON_FILE.exists():
        return False

    with LEGACY_JSON_FILE.open("r", encoding="utf-8") as fh:
        store = json.load(fh)

    seed_from_store(connection, store)
    connection.commit()
    LEGACY_JSON_FILE.rename(DATA_DIR / "store.migrated.json")
    return True


def sqlite_schema():
    return [
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            token TEXT NOT NULL,
            avatar_data TEXT,
            department TEXT,
            programme TEXT,
            group_id INTEGER,
            group_name TEXT,
            subgroup TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            credits INTEGER,
            hours INTEGER,
            description TEXT,
            year INTEGER,
            semester INTEGER,
            department TEXT,
            instructor_id INTEGER,
            instructor_name TEXT,
            programme TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teachers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            department TEXT,
            weekly_hours_limit INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT NOT NULL,
            capacity INTEGER,
            building TEXT,
            type TEXT,
            equipment TEXT,
            department TEXT,
            available INTEGER DEFAULT 1
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            student_count INTEGER NOT NULL,
            has_subgroups INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER,
            course_id INTEGER,
            course_name TEXT NOT NULL,
            teacher_id INTEGER,
            teacher_name TEXT NOT NULL,
            room_id INTEGER,
            room_number TEXT NOT NULL,
            group_id INTEGER,
            group_name TEXT,
            subgroup TEXT,
            day TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            semester INTEGER,
            year INTEGER,
            algorithm TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER NOT NULL,
            course_name TEXT NOT NULL,
            group_id INTEGER,
            group_name TEXT,
            classes_count INTEGER NOT NULL
        )
        """,
    ]


def postgres_schema():
    return [
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            token TEXT NOT NULL,
            avatar_data TEXT,
            department TEXT,
            programme TEXT,
            group_id INTEGER,
            group_name TEXT,
            subgroup TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS courses (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            credits INTEGER,
            hours INTEGER,
            description TEXT,
            year INTEGER,
            semester INTEGER,
            department TEXT,
            instructor_id INTEGER,
            instructor_name TEXT,
            programme TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teachers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            department TEXT,
            weekly_hours_limit INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            number TEXT NOT NULL,
            capacity INTEGER,
            building TEXT,
            type TEXT,
            equipment TEXT,
            department TEXT,
            available INTEGER DEFAULT 1
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS groups (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            student_count INTEGER NOT NULL,
            has_subgroups INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS schedules (
            id SERIAL PRIMARY KEY,
            section_id INTEGER,
            course_id INTEGER,
            course_name TEXT NOT NULL,
            teacher_id INTEGER,
            teacher_name TEXT NOT NULL,
            room_id INTEGER,
            room_number TEXT NOT NULL,
            group_id INTEGER,
            group_name TEXT,
            subgroup TEXT,
            day TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            semester INTEGER,
            year INTEGER,
            algorithm TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sections (
            id SERIAL PRIMARY KEY,
            course_id INTEGER NOT NULL,
            course_name TEXT NOT NULL,
            group_id INTEGER,
            group_name TEXT,
            classes_count INTEGER NOT NULL
        )
        """,
    ]


def migrate_user_email(connection, role, old_email, new_email):
    existing_new_email = query_one(
        connection,
        "SELECT id FROM users WHERE lower(email) = lower(?)",
        (new_email,),
    )
    if existing_new_email is not None:
        return

    db_execute(
        connection,
        """
        UPDATE users
        SET email = ?
        WHERE lower(email) = lower(?) AND role = ?
        """,
        (new_email, old_email, role),
    )


def migrate_default_user_emails(connection):
    migrate_user_email(
        connection,
        role="admin",
        old_email="admin@university.kz",
        new_email="admin@kazatu.edu.kz",
    )
    migrate_user_email(
        connection,
        role="teacher",
        old_email="teacher@university.kz",
        new_email="teacher@kazatu.edu.kz",
    )


def ensure_database():
    with get_connection() as connection:
        schema = postgres_schema() if DB_ENGINE == "postgres" else sqlite_schema()
        for statement in schema:
            db_execute(connection, statement)
        rename_column(connection, "users", "display_name", "full_name")
        rename_column(connection, "users", "programme_name", "programme")
        rename_column(connection, "courses", "study_year", "year")
        rename_column(connection, "courses", "programme_name", "programme")
        rename_column(connection, "teachers", "specialization", "department")
        rename_column(connection, "teachers", "max_hours_per_week", "weekly_hours_limit")
        rename_column(connection, "rooms", "is_available", "available")
        rename_column(connection, "sections", "class_count", "classes_count")
        ensure_column(connection, "users", "avatar_data", "TEXT")
        ensure_column(connection, "users", "department", "TEXT")
        ensure_column(connection, "users", "programme", "TEXT")
        ensure_column(connection, "users", "group_id", "INTEGER")
        ensure_column(connection, "users", "group_name", "TEXT")
        ensure_column(connection, "users", "subgroup", "TEXT")
        ensure_column(connection, "courses", "year", "INTEGER")
        ensure_column(connection, "courses", "semester", "INTEGER")
        ensure_column(connection, "courses", "department", "TEXT")
        ensure_column(connection, "courses", "instructor_id", "INTEGER")
        ensure_column(connection, "courses", "instructor_name", "TEXT")
        ensure_column(connection, "courses", "programme", "TEXT")
        ensure_column(connection, "teachers", "department", "TEXT")
        ensure_column(connection, "teachers", "weekly_hours_limit", "INTEGER")
        ensure_column(connection, "rooms", "department", "TEXT")
        ensure_column(connection, "rooms", "available", "INTEGER DEFAULT 1")
        ensure_column(connection, "groups", "has_subgroups", "INTEGER DEFAULT 0")
        ensure_column(connection, "sections", "group_id", "INTEGER")
        ensure_column(connection, "sections", "group_name", "TEXT")
        ensure_column(connection, "schedules", "section_id", "INTEGER")
        ensure_column(connection, "schedules", "group_id", "INTEGER")
        ensure_column(connection, "schedules", "group_name", "TEXT")
        ensure_column(connection, "schedules", "subgroup", "TEXT")
        migrate_default_user_emails(connection)
        connection.commit()

        counts = {
            table: query_scalar(connection, f"SELECT COUNT(*) FROM {table}")
            for table in ("users", "courses", "teachers", "rooms", "groups", "schedules", "sections")
        }

        if sum(counts.values()) == 0:
            if DB_ENGINE == "sqlite" and migrate_legacy_json(connection):
                return
            seed_from_store(connection, default_store())
            connection.commit()
