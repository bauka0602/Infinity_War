import base64
from io import BytesIO

from .auth_service import require_auth_user
from .collections import normalize_number_fields
from .config import DB_LOCK
from .db import db_execute, get_connection, insert_and_get_id, query_one
from .errors import ApiError

SHEET_ALIASES = {
    "courses": "courses",
    "course": "courses",
    "teachers": "teachers",
    "teacher": "teachers",
    "rooms": "rooms",
    "room": "rooms",
}

COURSE_HEADERS = {
    "name": "name",
    "course_name": "name",
    "название": "name",
    "атауы": "name",
    "code": "code",
    "course_code": "code",
    "код": "code",
    "credits": "credits",
    "credit": "credits",
    "кредиты": "credits",
    "кредит": "credits",
    "hours": "hours",
    "часы": "hours",
    "сағат": "hours",
    "description": "description",
    "описание": "description",
    "сипаттама": "description",
}

TEACHER_HEADERS = {
    "name": "name",
    "full_name": "name",
    "фио": "name",
    "аты-жөні": "name",
    "email": "email",
    "phone": "phone",
    "телефон": "phone",
    "specialization": "specialization",
    "специализация": "specialization",
    "мамандығы": "specialization",
    "max_hours_per_week": "max_hours_per_week",
    "max_hours": "max_hours_per_week",
    "максимум_часов_в_неделю": "max_hours_per_week",
    "апталық_сағат_лимиті": "max_hours_per_week",
}

ROOM_HEADERS = {
    "number": "number",
    "room_number": "number",
    "номер": "number",
    "нөмір": "number",
    "capacity": "capacity",
    "вместимость": "capacity",
    "сыйымдылығы": "capacity",
    "building": "building",
    "здание": "building",
    "ғимарат": "building",
    "type": "type",
    "тип": "type",
    "түрі": "type",
    "equipment": "equipment",
    "оборудование": "equipment",
    "жабдықтар": "equipment",
}

REQUIRED_FIELDS = {
    "courses": ["name", "code", "credits", "hours"],
    "teachers": ["name", "email"],
    "rooms": ["number", "capacity"],
}

ROOM_TYPE_ALIASES = {
    "lecture": "lecture",
    "lecturehall": "lecture",
    "lecture hall": "lecture",
    "лекция": "lecture",
    "лекционный": "lecture",
    "лекционная аудитория": "lecture",
    "practical": "practical",
    "practicalroom": "practical",
    "practical room": "practical",
    "practice": "practical",
    "практика": "practical",
    "практический": "practical",
    "практикалық": "practical",
    "lab": "lab",
    "laboratory": "lab",
    "лаборатория": "lab",
    "зертхана": "lab",
    "seminar": "seminar",
    "семинар": "seminar",
}


def _load_workbook(file_bytes):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ApiError(
            500,
            "internal_server_error",
            "Excel import dependency is not installed on the server.",
        ) from exc

    try:
        return load_workbook(filename=BytesIO(file_bytes), data_only=True)
    except Exception as exc:
        raise ApiError(
            400,
            "bad_request",
            "Не удалось прочитать Excel файл. Используйте формат .xlsx.",
        ) from exc


def _decode_excel_payload(payload):
    file_name = (payload.get("fileName") or "").strip()
    file_content = payload.get("fileContent")

    if not file_name or not file_content:
        raise ApiError(
            400,
            "fill_required_fields",
            "Заполните поля: fileName, fileContent",
            {"fields": ["fileName", "fileContent"]},
        )

    if not file_name.lower().endswith(".xlsx"):
        raise ApiError(
            400,
            "bad_request",
            "Поддерживаются только Excel файлы формата .xlsx.",
        )

    if "," in file_content:
        file_content = file_content.split(",", 1)[1]

    try:
        return base64.b64decode(file_content)
    except Exception as exc:
        raise ApiError(400, "bad_request", "Некорректное содержимое файла.") from exc


def _normalize_header(value):
    if value is None:
        return ""
    return str(value).strip().lower().replace("\n", " ").replace("-", "_")


def _normalize_sheet_name(sheet_name):
    normalized = _normalize_header(sheet_name).replace(" ", "")
    return SHEET_ALIASES.get(normalized)


def _normalize_room_type(value):
    if value in (None, ""):
        return ""
    normalized = _normalize_header(value).replace("_", " ")
    compact = normalized.replace(" ", "")
    return (
        ROOM_TYPE_ALIASES.get(compact)
        or ROOM_TYPE_ALIASES.get(normalized)
        or str(value).strip().lower()
    )


def _read_sheet_rows(sheet, header_aliases):
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        return []

    raw_headers = rows[0]
    canonical_headers = []
    for raw_header in raw_headers:
        normalized = _normalize_header(raw_header)
        canonical_headers.append(header_aliases.get(normalized, normalized))

    parsed_rows = []
    for row_index, row in enumerate(rows[1:], start=2):
        if not any(cell not in (None, "") for cell in row):
            continue

        parsed = {}
        for column_index, value in enumerate(row):
            if column_index >= len(canonical_headers):
                continue
            header = canonical_headers[column_index]
            if not header:
                continue
            parsed[header] = value.strip() if isinstance(value, str) else value
        parsed_rows.append((row_index, parsed))

    return parsed_rows


def _validate_required_fields(entity_name, row_index, payload):
    missing = [
        field for field in REQUIRED_FIELDS[entity_name] if payload.get(field) in (None, "")
    ]
    if missing:
        raise ApiError(
            400,
            "bad_request",
            f"Лист {entity_name}: строка {row_index}. Отсутствуют поля: {', '.join(missing)}.",
        )


def _upsert_course(connection, payload):
    normalized = normalize_number_fields(payload, ["credits", "hours"])
    existing = query_one(
        connection,
        "SELECT id FROM courses WHERE lower(code) = lower(?)",
        (normalized["code"],),
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE courses
            SET name = ?, code = ?, credits = ?, hours = ?, description = ?
            WHERE id = ?
            """,
            (
                normalized["name"],
                normalized["code"],
                normalized["credits"],
                normalized["hours"],
                normalized.get("description", "") or "",
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO courses (name, code, credits, hours, description)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            normalized["name"],
            normalized["code"],
            normalized["credits"],
            normalized["hours"],
            normalized.get("description", "") or "",
        ),
    )
    return "inserted"


def _upsert_teacher(connection, payload):
    normalized = normalize_number_fields(payload, ["max_hours_per_week"])
    existing = query_one(
        connection,
        "SELECT id FROM teachers WHERE lower(email) = lower(?)",
        (normalized["email"],),
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE teachers
            SET name = ?, email = ?, phone = ?, specialization = ?, max_hours_per_week = ?
            WHERE id = ?
            """,
            (
                normalized["name"],
                normalized["email"],
                normalized.get("phone", "") or "",
                normalized.get("specialization", "") or "",
                normalized.get("max_hours_per_week"),
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO teachers (name, email, phone, specialization, max_hours_per_week)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            normalized["name"],
            normalized["email"],
            normalized.get("phone", "") or "",
            normalized.get("specialization", "") or "",
            normalized.get("max_hours_per_week"),
        ),
    )
    return "inserted"


def _upsert_room(connection, payload):
    normalized = normalize_number_fields(payload, ["capacity"])
    normalized["type"] = _normalize_room_type(normalized.get("type"))
    existing = query_one(
        connection,
        "SELECT id FROM rooms WHERE number = ?",
        (str(normalized["number"]),),
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE rooms
            SET number = ?, capacity = ?, building = ?, type = ?, equipment = ?
            WHERE id = ?
            """,
            (
                str(normalized["number"]),
                normalized["capacity"],
                normalized.get("building", "") or "",
                normalized.get("type", "") or "",
                normalized.get("equipment", "") or "",
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO rooms (number, capacity, building, type, equipment)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(normalized["number"]),
            normalized["capacity"],
            normalized.get("building", "") or "",
            normalized.get("type", "") or "",
            normalized.get("equipment", "") or "",
        ),
    )
    return "inserted"


def import_excel_data(headers, payload):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    workbook = _load_workbook(_decode_excel_payload(payload))
    sheet_map = {
        "courses": COURSE_HEADERS,
        "teachers": TEACHER_HEADERS,
        "rooms": ROOM_HEADERS,
    }
    recognized_sheets = []
    summary = {
        "courses": {"inserted": 0, "updated": 0},
        "teachers": {"inserted": 0, "updated": 0},
        "rooms": {"inserted": 0, "updated": 0},
    }

    with DB_LOCK:
        with get_connection() as connection:
            for sheet in workbook.worksheets:
                entity_name = _normalize_sheet_name(sheet.title)
                if not entity_name:
                    continue

                recognized_sheets.append(sheet.title)
                rows = _read_sheet_rows(sheet, sheet_map[entity_name])
                for row_index, row_payload in rows:
                    _validate_required_fields(entity_name, row_index, row_payload)
                    if entity_name == "courses":
                        result = _upsert_course(connection, row_payload)
                    elif entity_name == "teachers":
                        result = _upsert_teacher(connection, row_payload)
                    else:
                        result = _upsert_room(connection, row_payload)
                    summary[entity_name][result] += 1

            if not recognized_sheets:
                raise ApiError(
                    400,
                    "bad_request",
                    "В Excel не найдены листы Courses, Teachers или Rooms.",
                )

            connection.commit()

    total_inserted = sum(item["inserted"] for item in summary.values())
    total_updated = sum(item["updated"] for item in summary.values())
    return {
        "message": "Excel import completed successfully.",
        "recognizedSheets": recognized_sheets,
        "summary": summary,
        "totals": {
            "inserted": total_inserted,
            "updated": total_updated,
        },
    }
