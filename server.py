import hashlib
import json
import os
import secrets
import sqlite3
import threading
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import parse_qs, urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None


def load_env_file(env_path):
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"

load_env_file(PROJECT_ROOT / ".env")
load_env_file(BASE_DIR / ".env")

DB_FILE = Path(os.getenv("SQLITE_DB_FILE", DATA_DIR / "timetable.db"))
LEGACY_JSON_FILE = DATA_DIR / "store.json"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_ENGINE = (
    "postgres"
    if DATABASE_URL.startswith(("postgres://", "postgresql://"))
    else "sqlite"
)
HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
PORT = int(os.getenv("PORT") or os.getenv("BACKEND_PORT", "8000"))
PASSWORD_PREFIX = "sha256$"
TEACHER_EMAIL_DOMAIN = "@kazatu.edu.kz"
EMAIL_CODE_PREFIX = "sha256$"
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "").strip()
REGISTRATION_CODE_TTL_MINUTES = int(os.getenv("REGISTRATION_CODE_TTL_MINUTES", "10"))

raw_allowed_origins = os.getenv("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS = [origin.strip() for origin in raw_allowed_origins.split(",") if origin.strip()]

DB_LOCK = threading.Lock()


def default_store():
    return {
        "users": [
            {
                "email": "admin@kazatu.edu.kz",
                "password": "admin123",
                "displayName": "System Admin",
                "role": "admin",
                "token": "seed-admin-token",
            },
            {
                "email": "teacher@kazatu.edu.kz",
                "password": "teacher123",
                "displayName": "Default Teacher",
                "role": "teacher",
                "token": "seed-teacher-token",
            },
            {
                "email": "student@university.kz",
                "password": "student123",
                "displayName": "Default Student",
                "role": "student",
                "token": "seed-student-token",
            },
        ],
        "courses": [
            {
                "name": "Algorithms",
                "code": "CS201",
                "credits": 4,
                "hours": 48,
                "description": "Core algorithms course",
            },
            {
                "name": "Databases",
                "code": "CS205",
                "credits": 3,
                "hours": 36,
                "description": "Relational database systems",
            },
        ],
        "teachers": [
            {
                "name": "Aruzhan Sarsembayeva",
                "email": "a.sarsembayeva@university.kz",
                "phone": "+7 701 000 0001",
                "specialization": "Computer Science",
                "max_hours_per_week": 20,
            },
            {
                "name": "Daniyar Omarov",
                "email": "d.omarov@university.kz",
                "phone": "+7 701 000 0002",
                "specialization": "Information Systems",
                "max_hours_per_week": 18,
            },
        ],
        "rooms": [
            {
                "number": "101",
                "capacity": 40,
                "building": "Main",
                "type": "lecture",
                "equipment": "Projector, speakers",
            },
            {
                "number": "Lab-3",
                "capacity": 24,
                "building": "Engineering",
                "type": "lab",
                "equipment": "24 PCs",
            },
        ],
        "schedules": [],
    }


def hash_password(password):
    digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return f"{PASSWORD_PREFIX}{digest}"


def verify_password(stored_password, plain_password):
    if stored_password.startswith(PASSWORD_PREFIX):
        return stored_password == hash_password(plain_password)
    return stored_password == plain_password


def hash_email_code(code):
    digest = hashlib.sha256(code.encode("utf-8")).hexdigest()
    return f"{EMAIL_CODE_PREFIX}{digest}"


def verify_email_code(code_hash, code):
    if not code_hash or not code:
        return False
    if code_hash.startswith(EMAIL_CODE_PREFIX):
        return code_hash == hash_email_code(code)
    return code_hash == code


def utc_now():
    return datetime.now(timezone.utc)


def generate_email_code():
    return f"{secrets.randbelow(1000000):06d}"


def parse_datetime(value):
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def format_datetime(value):
    return value.astimezone(timezone.utc).isoformat()


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


def sanitize_user(row):
    return {
        "id": row["id"],
        "email": row["email"],
        "displayName": row["display_name"],
        "role": row["role"],
        "token": row["token"],
    }


def parse_bearer_token(header_value):
    if not header_value:
        return None
    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def normalize_number_fields(payload, fields):
    normalized = deepcopy(payload)
    for field in fields:
        if field in normalized and normalized[field] not in ("", None):
            try:
                normalized[field] = int(normalized[field])
            except (TypeError, ValueError):
                pass
    return normalized


def monday_for_week(target_year):
    today = date.today()
    anchor = date(target_year, today.month, today.day)
    return anchor - timedelta(days=anchor.weekday())


def seed_from_store(connection, store):
    for user in store["users"]:
        db_execute(
            connection,
            """
            INSERT INTO users (email, password, display_name, role, token)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                user["email"],
                hash_password(user["password"]),
                user["displayName"],
                user["role"],
                user["token"],
            ),
        )

    for course in store["courses"]:
        db_execute(
            connection,
            """
            INSERT INTO courses (name, code, credits, hours, description)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                course["name"],
                course["code"],
                course["credits"],
                course["hours"],
                course.get("description", ""),
            ),
        )

    for teacher in store["teachers"]:
        db_execute(
            connection,
            """
            INSERT INTO teachers (name, email, phone, specialization, max_hours_per_week)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                teacher["name"],
                teacher["email"],
                teacher.get("phone", ""),
                teacher.get("specialization", ""),
                teacher.get("max_hours_per_week"),
            ),
        )

    for room in store["rooms"]:
        db_execute(
            connection,
            """
            INSERT INTO rooms (number, capacity, building, type, equipment)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                room["number"],
                room["capacity"],
                room.get("building", ""),
                room.get("type", ""),
                room.get("equipment", ""),
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
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            token TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS email_verification_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            role TEXT NOT NULL,
            purpose TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            is_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            credits INTEGER,
            hours INTEGER,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teachers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            specialization TEXT,
            max_hours_per_week INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT NOT NULL,
            capacity INTEGER,
            building TEXT,
            type TEXT,
            equipment TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER,
            course_name TEXT NOT NULL,
            teacher_id INTEGER,
            teacher_name TEXT NOT NULL,
            room_id INTEGER,
            room_number TEXT NOT NULL,
            day TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            semester INTEGER,
            year INTEGER,
            algorithm TEXT
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
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            token TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS email_verification_codes (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT NOT NULL,
            purpose TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            is_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS courses (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            credits INTEGER,
            hours INTEGER,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teachers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            specialization TEXT,
            max_hours_per_week INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            number TEXT NOT NULL,
            capacity INTEGER,
            building TEXT,
            type TEXT,
            equipment TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS schedules (
            id SERIAL PRIMARY KEY,
            course_id INTEGER,
            course_name TEXT NOT NULL,
            teacher_id INTEGER,
            teacher_name TEXT NOT NULL,
            room_id INTEGER,
            room_number TEXT NOT NULL,
            day TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            semester INTEGER,
            year INTEGER,
            algorithm TEXT
        )
        """,
    ]


def ensure_database():
    with get_connection() as connection:
        schema = postgres_schema() if DB_ENGINE == "postgres" else sqlite_schema()
        for statement in schema:
            db_execute(connection, statement)
        migrate_default_user_emails(connection)
        connection.commit()

        counts = {
            table: query_scalar(connection, f"SELECT COUNT(*) FROM {table}")
            for table in ("users", "email_verification_codes", "courses", "teachers", "rooms", "schedules")
        }

        if sum(counts.values()) == 0:
            if DB_ENGINE == "sqlite" and migrate_legacy_json(connection):
                return
            seed_from_store(connection, default_store())
            connection.commit()


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


def ensure_teacher_email_allowed(email, role):
    if role == "teacher" and not email.lower().endswith(TEACHER_EMAIL_DOMAIN):
        raise ValueError("Для преподавателя нужен email, оканчивающийся на @kazatu.edu.kz")


def send_verification_email(email, code, role):
    if not RESEND_API_KEY:
        raise ValueError("Не настроен RESEND_API_KEY")
    if not RESEND_FROM_EMAIL:
        raise ValueError("Не настроен RESEND_FROM_EMAIL")

    role_label = "преподавателя" if role == "teacher" else "студента"
    subject = "Код подтверждения регистрации"
    text = (
        f"Ваш код подтверждения для регистрации {role_label}: {code}\n\n"
        f"Код действует {REGISTRATION_CODE_TTL_MINUTES} минут."
    )
    payload = json.dumps(
        {
            "from": RESEND_FROM_EMAIL,
            "to": [email],
            "subject": subject,
            "text": text,
        }
    ).encode("utf-8")
    request = urllib_request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(request, timeout=20) as response:
            status_code = response.getcode()
            if status_code < 200 or status_code >= 300:
                raise ValueError("Письмо не было отправлено")
    except urllib_error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="ignore")
        raise ValueError(
            f"Ошибка отправки письма через Resend: {details or exc.reason}"
        ) from exc
    except urllib_error.URLError as exc:
        raise ValueError(f"Не удалось подключиться к Resend: {exc.reason}") from exc


def create_registration_code(connection, email, role):
    code = generate_email_code()
    expires_at = utc_now() + timedelta(minutes=REGISTRATION_CODE_TTL_MINUTES)
    created_at = utc_now()

    db_execute(
        connection,
        """
        UPDATE email_verification_codes
        SET is_used = 1
        WHERE lower(email) = lower(?) AND role = ? AND purpose = ? AND is_used = 0
        """,
        (email, role, "register"),
    )
    db_execute(
        connection,
        """
        INSERT INTO email_verification_codes (
            email, role, purpose, code_hash, expires_at, is_used, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            email,
            role,
            "register",
            hash_email_code(code),
            format_datetime(expires_at),
            0,
            format_datetime(created_at),
        ),
    )
    return code


def get_active_registration_code(connection, email, role):
    return query_one(
        connection,
        """
        SELECT id, code_hash, expires_at, is_used
        FROM email_verification_codes
        WHERE lower(email) = lower(?) AND role = ? AND purpose = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (email, role, "register"),
    )


def verify_registration_code(connection, email, role, code):
    verification = get_active_registration_code(connection, email, role)
    if verification is None:
        raise ValueError("Сначала запросите код подтверждения")
    if verification["is_used"]:
        raise ValueError("Этот код уже использован. Запросите новый код")

    expires_at = parse_datetime(verification["expires_at"])
    if expires_at is None or utc_now() > expires_at:
        raise ValueError("Срок действия кода истёк. Запросите новый код")

    if not verify_email_code(verification["code_hash"], code):
        raise ValueError("Неверный код подтверждения")

    db_execute(
        connection,
        "UPDATE email_verification_codes SET is_used = 1 WHERE id = ?",
        (verification["id"],),
    )


def list_collection(connection, collection, query):
    if collection == "users":
        return query_all(
            connection,
            """
            SELECT id, email, display_name AS displayName, role, token
            FROM users
            ORDER BY id
            """,
        )

    if collection == "courses":
        return query_all(
            connection,
            """
            SELECT id, name, code, credits, hours, description
            FROM courses
            ORDER BY id
            """,
        )

    if collection == "teachers":
        return query_all(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            ORDER BY id
            """,
        )

    if collection == "rooms":
        return query_all(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            ORDER BY id
            """,
        )

    clauses = []
    params = []
    semester = query.get("semester", [None])[0]
    year = query.get("year", [None])[0]
    if semester is not None:
        clauses.append("semester = ?")
        params.append(semester)
    if year is not None:
        clauses.append("year = ?")
        params.append(year)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return query_all(
        connection,
        f"""
        SELECT
            id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
            day, start_hour, semester, year, algorithm
        FROM schedules
        {where_sql}
        ORDER BY day, start_hour, id
        """,
        tuple(params),
    )


def create_collection_item(connection, collection, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO courses (name, code, credits, hours, description)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("code"),
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            "SELECT id, name, code, credits, hours, description FROM courses WHERE id = ?",
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["max_hours_per_week"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (name, email, phone, specialization, max_hours_per_week)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("specialization", ""),
                normalized.get("max_hours_per_week"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity"])
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO rooms (number, capacity, building, type, equipment)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["course_id", "teacher_id", "room_id", "start_hour", "semester", "year"],
        )
        item_id = insert_and_get_id(
            connection,
            """
            INSERT INTO schedules (
                course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("algorithm"),
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ValueError("Unsupported collection")


def update_collection_item(connection, collection, item_id, payload):
    if collection == "courses":
        normalized = normalize_number_fields(payload, ["credits", "hours"])
        db_execute(
            connection,
            """
            UPDATE courses
            SET name = ?, code = ?, credits = ?, hours = ?, description = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("code"),
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("description", ""),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            "SELECT id, name, code, credits, hours, description FROM courses WHERE id = ?",
            (item_id,),
        )

    if collection == "teachers":
        normalized = normalize_number_fields(payload, ["max_hours_per_week"])
        db_execute(
            connection,
            """
            UPDATE teachers
            SET name = ?, email = ?, phone = ?, specialization = ?, max_hours_per_week = ?
            WHERE id = ?
            """,
            (
                normalized.get("name"),
                normalized.get("email"),
                normalized.get("phone", ""),
                normalized.get("specialization", ""),
                normalized.get("max_hours_per_week"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, name, email, phone, specialization, max_hours_per_week
            FROM teachers
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "rooms":
        normalized = normalize_number_fields(payload, ["capacity"])
        db_execute(
            connection,
            """
            UPDATE rooms
            SET number = ?, capacity = ?, building = ?, type = ?, equipment = ?
            WHERE id = ?
            """,
            (
                normalized.get("number"),
                normalized.get("capacity"),
                normalized.get("building", ""),
                normalized.get("type", ""),
                normalized.get("equipment", ""),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT id, number, capacity, building, type, equipment
            FROM rooms
            WHERE id = ?
            """,
            (item_id,),
        )

    if collection == "schedules":
        normalized = normalize_number_fields(
            payload,
            ["course_id", "teacher_id", "room_id", "start_hour", "semester", "year"],
        )
        db_execute(
            connection,
            """
            UPDATE schedules
            SET
                course_id = ?, course_name = ?, teacher_id = ?, teacher_name = ?,
                room_id = ?, room_number = ?, day = ?, start_hour = ?,
                semester = ?, year = ?, algorithm = ?
            WHERE id = ?
            """,
            (
                normalized.get("course_id"),
                normalized.get("course_name"),
                normalized.get("teacher_id"),
                normalized.get("teacher_name"),
                normalized.get("room_id"),
                normalized.get("room_number"),
                normalized.get("day"),
                normalized.get("start_hour"),
                normalized.get("semester"),
                normalized.get("year"),
                normalized.get("algorithm"),
                item_id,
            ),
        )
        connection.commit()
        return query_one(
            connection,
            """
            SELECT
                id, course_id, course_name, teacher_id, teacher_name, room_id, room_number,
                day, start_hour, semester, year, algorithm
            FROM schedules
            WHERE id = ?
            """,
            (item_id,),
        )

    raise ValueError("Unsupported collection")


def delete_collection_item(connection, collection, item_id):
    db_execute(connection, f"DELETE FROM {collection} WHERE id = ?", (item_id,))
    connection.commit()


def build_schedule(connection, semester, year, algorithm):
    courses = query_all(connection, "SELECT id, name FROM courses ORDER BY id")
    teachers = query_all(connection, "SELECT id, name FROM teachers ORDER BY id")
    rooms = query_all(connection, "SELECT id, number FROM rooms ORDER BY id")

    if not courses or not teachers or not rooms:
        raise ValueError("Для генерации расписания нужны курсы, преподаватели и аудитории.")

    start_day = monday_for_week(year)
    slots = [(day_idx, hour) for day_idx in range(6) for hour in range(8, 18)]
    generated = []

    for idx, course in enumerate(courses):
        day_idx, hour = slots[idx % len(slots)]
        teacher = teachers[idx % len(teachers)]
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


def resolve_allowed_origin(request_origin):
    if "*" in ALLOWED_ORIGINS:
        return "*"
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return None


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "TimeTableGBackend/3.0"

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_headers()
        self.end_headers()

    def do_GET(self):
        self.route_request("GET")

    def do_POST(self):
        self.route_request("POST")

    def do_PUT(self):
        self.route_request("PUT")

    def do_DELETE(self):
        self.route_request("DELETE")

    def _set_headers(self, content_type="application/json"):
        allowed_origin = resolve_allowed_origin(self.headers.get("Origin"))
        self.send_header("Content-Type", content_type)
        if allowed_origin:
            self.send_header("Access-Control-Allow-Origin", allowed_origin)
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        if allowed_origin and allowed_origin != "*":
            self.send_header("Vary", "Origin")

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._set_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}
        raw_body = self.rfile.read(content_length).decode("utf-8")
        return json.loads(raw_body) if raw_body else {}

    def route_request(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if not path.startswith("/api"):
            self.send_json(404, {"error": "Not found"})
            return

        api_path = path[4:] or "/"

        try:
            if api_path == "/health" and method == "GET":
                self.send_json(200, {"status": "ok", "engine": DB_ENGINE})
                return

            if api_path == "/auth/register" and method == "POST":
                self.handle_register()
                return

            if api_path == "/auth/request-registration-code" and method == "POST":
                self.handle_request_registration_code()
                return

            if api_path == "/auth/login" and method == "POST":
                self.handle_login()
                return

            if api_path == "/auth/logout" and method == "POST":
                self.send_json(200, {"success": True})
                return

            if api_path == "/schedules/generate" and method == "POST":
                self.handle_schedule_generation()
                return

            self.handle_collection_routes(method, api_path, parse_qs(parsed.query))
        except ValueError as exc:
            self.send_json(400, {"error": str(exc)})
        except json.JSONDecodeError:
            self.send_json(400, {"error": "Некорректный JSON"})
        except (sqlite3.IntegrityError, Exception) as exc:
            is_integrity = exc.__class__.__name__ in {"IntegrityError", "UniqueViolation"}
            if is_integrity:
                self.send_json(400, {"error": f"Ошибка БД: {exc}"})
                return
            self.send_json(500, {"error": f"Внутренняя ошибка сервера: {exc}"})

    def handle_collection_routes(self, method, api_path, query):
        parts = [part for part in api_path.split("/") if part]
        if not parts:
            self.send_json(404, {"error": "Not found"})
            return

        collection = parts[0]
        if collection not in {"courses", "teachers", "rooms", "schedules"}:
            self.send_json(404, {"error": "Not found"})
            return

        user = self.require_auth()
        if user is None:
            return

        if collection in {"courses", "teachers", "rooms"} and user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав"})
            return

        if collection == "schedules" and method in {"POST", "PUT", "DELETE"} and user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав"})
            return

        with DB_LOCK:
            with get_connection() as connection:
                if len(parts) == 1:
                    if method == "GET":
                        self.send_json(200, list_collection(connection, collection, query))
                        return

                    if method == "POST":
                        created = create_collection_item(connection, collection, self.read_json())
                        self.send_json(201, created)
                        return

                if len(parts) == 2:
                    try:
                        item_id = int(parts[1])
                    except ValueError as exc:
                        raise ValueError("ID должен быть числом") from exc

                    existing = query_one(
                        connection,
                        f"SELECT id FROM {collection} WHERE id = ?",
                        (item_id,),
                    )
                    if existing is None:
                        self.send_json(404, {"error": "Запись не найдена"})
                        return

                    if method == "PUT":
                        updated = update_collection_item(
                            connection,
                            collection,
                            item_id,
                            self.read_json(),
                        )
                        self.send_json(200, updated)
                        return

                    if method == "DELETE":
                        delete_collection_item(connection, collection, item_id)
                        self.send_json(200, {"success": True})
                        return

        self.send_json(405, {"error": "Method not allowed"})

    def require_auth(self):
        token = parse_bearer_token(self.headers.get("Authorization"))
        if not token:
            self.send_json(401, {"error": "Требуется авторизация"})
            return None

        with DB_LOCK:
            with get_connection() as connection:
                user = query_one(
                    connection,
                    """
                    SELECT id, email, display_name, role, token
                    FROM users
                    WHERE token = ?
                    """,
                    (token,),
                )

        if user is None:
            self.send_json(401, {"error": "Недействительный токен"})
            return None

        return user

    def handle_register(self):
        payload = self.read_json()
        required = ["email", "password", "displayName", "verificationCode"]
        missing = [field for field in required if not payload.get(field)]
        if missing:
            raise ValueError(f"Заполните поля: {', '.join(missing)}")

        role = (payload.get("role") or "student").strip().lower()
        email = payload["email"].strip()
        verification_code = payload["verificationCode"].strip()
        if role not in {"student", "teacher"}:
            raise ValueError("Можно зарегистрироваться только как студент или преподаватель")

        ensure_teacher_email_allowed(email, role)

        with DB_LOCK:
            with get_connection() as connection:
                existing = query_one(
                    connection,
                    "SELECT id FROM users WHERE lower(email) = lower(?)",
                    (email,),
                )
                if existing:
                    raise ValueError("Пользователь с таким email уже существует")

                verify_registration_code(connection, email, role, verification_code)
                token = secrets.token_urlsafe(32)
                user_id = insert_and_get_id(
                    connection,
                    """
                    INSERT INTO users (email, password, display_name, role, token)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        email,
                        hash_password(payload["password"]),
                        payload["displayName"],
                        role,
                        token,
                    ),
                )
                connection.commit()
                user = query_one(connection, "SELECT * FROM users WHERE id = ?", (user_id,))

        self.send_json(201, sanitize_user(user))

    def handle_request_registration_code(self):
        payload = self.read_json()
        email = payload.get("email", "").strip()
        role = (payload.get("role") or "student").strip().lower()

        if not email:
            raise ValueError("Введите email")
        if role not in {"student", "teacher"}:
            raise ValueError("Можно запросить код только для студента или преподавателя")

        ensure_teacher_email_allowed(email, role)

        with DB_LOCK:
            with get_connection() as connection:
                existing = query_one(
                    connection,
                    "SELECT id FROM users WHERE lower(email) = lower(?)",
                    (email,),
                )
                if existing:
                    raise ValueError("Пользователь с таким email уже существует")

                code = create_registration_code(connection, email, role)
                connection.commit()

        send_verification_email(email, code, role)
        self.send_json(
            200,
            {
                "success": True,
                "message": f"Код подтверждения отправлен на {email}",
            },
        )

    def handle_login(self):
        payload = self.read_json()
        email = payload.get("email", "").strip()
        password = payload.get("password", "")
        selected_role = (payload.get("role") or "").strip().lower()

        if selected_role and selected_role not in {"admin", "student", "teacher"}:
            raise ValueError("Некорректная роль")

        with DB_LOCK:
            with get_connection() as connection:
                user = query_one(
                    connection,
                    """
                    SELECT * FROM users
                    WHERE lower(email) = lower(?)
                    """,
                    (email,),
                )

        if user is None or not verify_password(user["password"], password):
            self.send_json(401, {"error": "Неверный email или пароль"})
            return

        if selected_role and user["role"] != selected_role:
            self.send_json(403, {"error": "Этот аккаунт зарегистрирован с другой ролью"})
            return

        if user["role"] == "teacher" and not user["email"].lower().endswith(TEACHER_EMAIL_DOMAIN):
            self.send_json(403, {"error": "У аккаунта преподавателя должен быть email @kazatu.edu.kz"})
            return

        self.send_json(200, sanitize_user(user))

    def handle_schedule_generation(self):
        user = self.require_auth()
        if user is None:
            return
        if user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав"})
            return

        payload = self.read_json()
        semester = int(payload.get("semester") or 1)
        year = int(payload.get("year") or date.today().year)
        algorithm = payload.get("algorithm") or "greedy"

        with DB_LOCK:
            with get_connection() as connection:
                generated = build_schedule(connection, semester, year, algorithm)

        self.send_json(200, generated)


def run():
    ensure_database()
    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend started at http://{HOST}:{PORT}")
    if DB_ENGINE == "postgres":
        print("Database engine: PostgreSQL")
    else:
        print(f"SQLite database: {DB_FILE}")
    server.serve_forever()


if __name__ == "__main__":
    run()
