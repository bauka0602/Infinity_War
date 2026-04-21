import base64
import re
from datetime import date
from io import BytesIO

from .auth_service import require_auth_user
from .collections import normalize_number_fields
from .config import DB_LOCK
from .db import db_execute, get_connection, insert_and_get_id, query_all, query_one
from .errors import ApiError

SHEET_ALIASES = {
    "courses": "courses",
    "course": "courses",
    "disciplines": "courses",
    "discipline": "courses",
    "teachers": "teachers",
    "teacher": "teachers",
    "rooms": "rooms",
    "room": "rooms",
    "groups": "groups",
    "group": "groups",
    "sections": "sections",
    "section": "sections",
}

COURSE_HEADERS = {
    "code": "code",
    "course_code": "code",
    "код": "code",
    "course_code": "code",
    "name": "name",
    "course_name": "name",
    "название": "name",
    "атауы": "name",
    "study_year": "year",
    "year": "year",
    "course": "year",
    "study_course": "year",
    "course_of_study": "year",
    "курс": "year",
    "год": "year",
    "semester": "semester",
    "семестр": "semester",
    "department": "department",
    "faculty": "department",
    "faculty_institute": "department",
    "faculty_or_institute": "department",
    "факультет": "department",
    "факультет_институт": "department",
    "институт": "department",
    "instructor": "instructor_name",
    "teacher": "instructor_name",
    "teacher_name": "instructor_name",
    "instructor_name": "instructor_name",
    "преподаватель": "instructor_name",
    "оқытушы": "instructor_name",
    "programme": "programme",
    "programme_name": "programme",
    "program_name": "programme",
    "program": "programme",
    "образовательная_программа": "programme",
    "бағдарлама": "programme",
    "module_type": "module_type",
    "тип_модуля": "module_type",
    "модульдің_түрі": "module_type",
    "module_name": "module_name",
    "наименование_модуля": "module_name",
    "модульдің_атауы": "module_name",
    "cycle": "cycle",
    "discipline_cycle": "cycle",
    "цикл_дисциплины": "cycle",
    "пәннің_циклы": "cycle",
    "component": "component",
    "discipline_component": "component",
    "компонент_дисциплины": "component",
    "пәннің_компонент": "component",
    "language": "language",
    "academic_year": "academic_year",
    "entry_year": "entry_year",
    "description": "description",
    "описание": "description",
    "сипаттама": "description",
    "requires_computers": "requires_computers",
    "computer_required": "requires_computers",
    "computers_required": "requires_computers",
    "pc_required": "requires_computers",
    "requires_pc": "requires_computers",
    "нужны_компьютеры": "requires_computers",
    "требуются_компьютеры": "requires_computers",
    "компьютер_қажет": "requires_computers",
}

TEACHER_HEADERS = {
    "name": "name",
    "full_name": "name",
    "фио": "name",
    "аты-жөні": "name",
    "email": "email",
    "phone": "phone",
    "телефон": "phone",
    "specialization": "department",
    "faculty": "department",
    "department": "department",
    "faculty_institute": "department",
    "faculty_or_institute": "department",
    "факультет": "department",
    "факультет_институт": "department",
    "институт": "department",
    "специализация": "department",
    "мамандығы": "department",
    "max_hours_per_week": "weekly_hours_limit",
    "max_hours": "weekly_hours_limit",
    "максимум_часов_в_неделю": "weekly_hours_limit",
    "апталық_сағат_лимиті": "weekly_hours_limit",
    "teaching_languages": "teaching_languages",
    "languages": "teaching_languages",
    "языки": "teaching_languages",
    "оқыту_тілдері": "teaching_languages",
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
    "department": "department",
    "faculty": "department",
    "faculty_institute": "department",
    "faculty_or_institute": "department",
    "факультет": "department",
    "факультет_институт": "department",
    "институт": "department",
    "available": "available",
    "is_available": "available",
    "доступно": "available",
    "қолжетімді": "available",
    "equipment": "equipment",
    "оборудование": "equipment",
    "жабдықтар": "equipment",
    "computer_count": "computer_count",
    "computers": "computer_count",
    "pc_count": "computer_count",
    "pcs": "computer_count",
    "компьютеры": "computer_count",
    "количество_компьютеров": "computer_count",
    "компьютер_саны": "computer_count",
}

GROUP_HEADERS = {
    "name": "name",
    "group_name": "name",
    "group_number": "name",
    "номер_группы": "name",
    "топ_нөмірі": "name",
    "student_count": "student_count",
    "students_count": "student_count",
    "количество_студентов": "student_count",
    "студент_саны": "student_count",
    "has_subgroups": "has_subgroups",
    "subgroups": "has_subgroups",
    "подгруппы": "has_subgroups",
    "language": "language",
    "lang": "language",
    "язык": "language",
    "оқыту_тілі": "language",
    "study_course": "study_course",
    "course": "study_course",
    "group_course": "study_course",
    "курс": "study_course",
    "оқу_курсы": "study_course",
}

SECTION_HEADERS = {
    "course_code": "course_code",
    "code": "course_code",
    "код_курса": "course_code",
    "group_name": "group_name",
    "group_number": "group_name",
    "номер_группы": "group_name",
    "топ_нөмірі": "group_name",
    "classes_count": "classes_count",
    "class_count": "classes_count",
    "количество_занятий": "classes_count",
    "сабақ_саны": "classes_count",
    "lesson_type": "lesson_type",
    "type": "lesson_type",
    "тип_занятия": "lesson_type",
    "сабақ_түрі": "lesson_type",
}

REQUIRED_FIELDS = {
    "courses": ["code", "name", "year", "semester", "programme", "department"],
    "teachers": ["name", "email"],
    "rooms": ["number", "capacity", "department"],
    "groups": ["name", "student_count", "study_course"],
    "sections": ["course_code", "group_name", "classes_count"],
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

LESSON_TYPE_ALIASES = {
    "lecture": "lecture",
    "лекция": "lecture",
    "дәріс": "lecture",
    "practical": "practical",
    "practice": "practical",
    "practical lesson": "practical",
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

TEMPLATE_HEADERS = {
    "Disciplines": [
        "code",
        "name",
        "course",
        "semester",
        "programme",
        "department",
        "instructor_name",
        "description",
        "requires_computers",
    ],
    "Teachers": ["name", "email", "phone", "department", "teaching_languages"],
    "Rooms": [
        "number",
        "capacity",
        "building",
        "type",
        "department",
        "available",
        "equipment",
        "computer_count",
    ],
    "Groups": ["name", "student_count", "study_course", "has_subgroups", "language"],
    "Sections": ["course_code", "group_name", "classes_count", "lesson_type"],
}

TEMPLATE_ROWS = {
    "Disciplines": [
        [
            "CS101",
            "Programming 1",
            1,
            1,
            "Программная инженерия (6B06101)",
            "Факультет компьютерных систем и профессионального образования (КСиПО-БжЦТ)",
            "Aruzhan Saparova",
            "Introduction to programming",
            "no",
        ],
    ],
    "Teachers": [
        [
            "Aruzhan Saparova",
            "aruzhan@kazatu.edu.kz",
            "+7 777 000 00 00",
            "Факультет компьютерных систем и профессионального образования (КСиПО-БжЦТ)",
            "ru,kk",
        ],
    ],
    "Rooms": [
        [
            "101",
            30,
            "Main Building",
            "lecture",
            "Факультет компьютерных систем и профессионального образования (КСиПО-БжЦТ)",
            "yes",
            "Projector, whiteboard",
            0,
        ],
    ],
    "Groups": [
        ["SE-23-01", 24, 2, "yes", "ru"],
    ],
    "Sections": [
        ["CS101", "SE-23-01", 2, "lecture"],
    ],
}

AVAILABLE_ALIASES = {
    "1": 1,
    "true": 1,
    "yes": 1,
    "available": 1,
    "да": 1,
    "иә": 1,
    "нет": 0,
    "no": 0,
    "false": 0,
    "0": 0,
    "not_available": 0,
    "not available": 0,
    "жоқ": 0,
}

ROP_PERIOD_COLUMN_GROUPS = (
    {
        "academic_period_column": 10,
        "total": 10,
        "lecture": 11,
        "practical": 12,
        "lab": 13,
        "studio": 14,
        "practice": 15,
        "srop": 16,
        "sro": 17,
    },
    {
        "academic_period_column": 18,
        "total": 18,
        "lecture": 19,
        "practical": 20,
        "lab": 21,
        "studio": 22,
        "practice": 23,
        "srop": 24,
        "sro": 25,
    },
)

ROP_LESSON_TYPES = ("lecture", "practical", "lab", "studio")
ROP_PC_REQUIRED_LESSON_TYPES = {"practical", "lab"}


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


def _decode_file_payload(payload, allowed_extensions, format_message):
    file_name = (payload.get("fileName") or "").strip()
    file_content = payload.get("fileContent")

    if not file_name or not file_content:
        raise ApiError(
            400,
            "fill_required_fields",
            "Заполните поля: fileName, fileContent",
            {"fields": ["fileName", "fileContent"]},
        )

    if not file_name.lower().endswith(tuple(allowed_extensions)):
        raise ApiError(
            400,
            "bad_request",
            format_message,
        )

    if "," in file_content:
        file_content = file_content.split(",", 1)[1]

    try:
        return file_name, base64.b64decode(file_content)
    except Exception as exc:
        raise ApiError(400, "bad_request", "Некорректное содержимое файла.") from exc


def _decode_excel_payload(payload):
    _file_name, file_bytes = _decode_file_payload(
        payload,
        (".xlsx",),
        "Поддерживаются только Excel файлы формата .xlsx.",
    )
    return file_bytes


def _normalize_header(value):
    if value is None:
        return ""
    return str(value).strip().lower().replace("\n", " ").replace("-", "_")


def _normalize_cell(value):
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        return value.strip()
    return value


def _cell_text(value):
    value = _normalize_cell(value)
    return str(value).strip() if value not in (None, "") else ""


def _number_or_none(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value) if float(value).is_integer() else float(value)
    normalized = str(value).replace(",", ".").strip()
    if not normalized:
        return None
    try:
        parsed = float(normalized)
    except ValueError:
        return None
    return int(parsed) if parsed.is_integer() else parsed


def _safe_row_value(row, index):
    return row[index] if index < len(row) else ""


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


def _normalize_availability(value):
    if value in (None, ""):
        return 1
    if isinstance(value, bool):
        return 1 if value else 0
    normalized = _normalize_header(value).replace("_", " ")
    compact = normalized.replace(" ", "_")
    return AVAILABLE_ALIASES.get(compact, AVAILABLE_ALIASES.get(normalized, 1 if value else 0))


def _normalize_bool_flag(value):
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    normalized = _normalize_header(value).replace("_", " ")
    compact = normalized.replace(" ", "_")
    truthy_values = {
        "1",
        "true",
        "yes",
        "да",
        "иә",
        "required",
        "needed",
        "need",
        "қажет",
    }
    return 1 if normalized in truthy_values or compact in truthy_values else 0


def _normalize_lesson_type(value):
    if value in (None, ""):
        return "lecture"
    normalized = _normalize_header(value).replace("_", " ")
    compact = normalized.replace(" ", "_")
    return LESSON_TYPE_ALIASES.get(compact, LESSON_TYPE_ALIASES.get(normalized, str(value).strip().lower()))


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
    normalized = normalize_number_fields(payload, ["credits", "hours", "year", "study_year", "semester"])
    requires_computers = _normalize_bool_flag(normalized.get("requires_computers"))
    instructor_name = (normalized.get("instructor_name") or "").strip()
    instructor_id = None
    if instructor_name:
        teacher = query_one(
            connection,
            """
            SELECT id, name
            FROM teachers
            WHERE lower(name) = lower(?)
            """,
            (instructor_name,),
        )
        if teacher:
            instructor_id = teacher["id"]
            instructor_name = teacher["name"]

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
            SET
                name = ?,
                code = ?,
                description = ?,
                credits = ?,
                hours = ?,
                year = ?,
                semester = ?,
                department = ?,
                instructor_id = ?,
                instructor_name = ?,
                programme = ?,
                module_type = ?,
                module_name = ?,
                cycle = ?,
                component = ?,
                language = ?,
                academic_year = ?,
                entry_year = ?,
                requires_computers = ?
            WHERE id = ?
            """,
            (
                normalized["name"],
                normalized["code"],
                normalized.get("description", "") or "",
                normalized.get("credits"),
                normalized.get("hours"),
                normalized.get("year", normalized.get("study_year")),
                normalized.get("semester"),
                normalized.get("department", "") or "",
                instructor_id,
                instructor_name,
                normalized.get("programme", normalized.get("programme_name", "")) or "",
                normalized.get("module_type", "") or "",
                normalized.get("module_name", "") or "",
                normalized.get("cycle", "") or "",
                normalized.get("component", "") or "",
                normalized.get("language", "") or "",
                normalized.get("academic_year", "") or "",
                normalized.get("entry_year", "") or "",
                requires_computers,
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO courses (
            name, code, credits, hours, description,
            year, semester, department, instructor_id, instructor_name, programme,
            module_type, module_name, cycle, component, language, academic_year, entry_year,
            requires_computers
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalized["name"],
            normalized["code"],
            normalized.get("credits"),
            normalized.get("hours"),
            normalized.get("description", "") or "",
            normalized.get("year", normalized.get("study_year")),
            normalized.get("semester"),
            normalized.get("department", "") or "",
            instructor_id,
            instructor_name,
            normalized.get("programme", normalized.get("programme_name", "")) or "",
            normalized.get("module_type", "") or "",
            normalized.get("module_name", "") or "",
            normalized.get("cycle", "") or "",
            normalized.get("component", "") or "",
            normalized.get("language", "") or "",
            normalized.get("academic_year", "") or "",
            normalized.get("entry_year", "") or "",
            requires_computers,
        ),
    )
    return "inserted"


def _upsert_rop_course(connection, course, offering):
    existing = query_one(
        connection,
        """
        SELECT id
        FROM courses
        WHERE lower(code) = lower(?) AND lower(name) = lower(?) AND semester = ? AND year = ? AND lower(programme) = lower(?)
        """,
        (
            course["code"],
            course["name"],
            offering["academicPeriod"],
            course.get("studyYear"),
            course.get("programme") or "",
        ),
    )
    description = (
        f"Imported from ROP. Component: {course.get('component') or '-'}; "
        f"cycle: {course.get('cycle') or '-'}; academic period: {offering['academicPeriod']}."
    )
    params = (
        course["name"],
        course["code"],
        course.get("credits"),
        offering.get("totalHours"),
        description,
        course.get("studyYear"),
        offering["academicPeriod"],
        "",
        None,
        "",
        course.get("programme") or "",
        course.get("moduleType") or "",
        course.get("moduleName") or "",
        course.get("cycle") or "",
        course.get("component") or "",
        course.get("language") or "",
        course.get("academicYear") or "",
        course.get("entryYear") or "",
        0,
    )

    if existing:
        db_execute(
            connection,
            """
            UPDATE courses
            SET
                name = ?,
                code = ?,
                credits = ?,
                hours = ?,
                description = ?,
                year = ?,
                semester = ?,
                department = ?,
                instructor_id = ?,
                instructor_name = ?,
                programme = ?,
                module_type = ?,
                module_name = ?,
                cycle = ?,
                component = ?,
                language = ?,
                academic_year = ?,
                entry_year = ?,
                requires_computers = ?
            WHERE id = ?
            """,
            (*params, existing["id"]),
        )
        return "updated", existing["id"]

    course_id = insert_and_get_id(
        connection,
        """
        INSERT INTO courses (
            name, code, credits, hours, description,
            year, semester, department, instructor_id, instructor_name, programme,
            module_type, module_name, cycle, component, language, academic_year, entry_year,
            requires_computers
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        params,
    )
    return "inserted", course_id


def _replace_rop_course_components(connection, course_id, course, offering, lesson_components):
    db_execute(
        connection,
        """
        DELETE FROM course_components
        WHERE course_id = ? AND academic_period = ?
        """,
        (course_id, offering["academicPeriod"]),
    )

    inserted = 0
    for component in lesson_components:
        if (
            component["courseCode"] != course["code"]
            or component["courseName"] != course["name"]
            or component["academicPeriod"] != offering["academicPeriod"]
        ):
            continue

        insert_and_get_id(
            connection,
            """
            INSERT INTO course_components (
                course_id, course_code, course_name, programme, study_year,
                academic_period, semester, lesson_type, hours, weekly_classes,
                requires_computers
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                course_id,
                component["courseCode"],
                component["courseName"],
                course.get("programme") or "",
                course.get("studyYear"),
                component["academicPeriod"],
                component["semester"],
                component["lessonType"],
                component["hours"],
                component["weeklyClasses"],
                1 if component.get("requiresComputers") else 0,
            ),
        )
        inserted += 1

    return inserted


def _upsert_teacher(connection, payload):
    normalized = normalize_number_fields(payload, ["weekly_hours_limit", "max_hours_per_week"])
    teaching_languages = _normalize_teaching_languages(normalized.get("teaching_languages"))
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
            SET name = ?, email = ?, phone = ?, department = ?, weekly_hours_limit = ?, teaching_languages = ?
            WHERE id = ?
            """,
            (
                normalized["name"],
                normalized["email"],
                normalized.get("phone", "") or "",
                normalized.get("department", normalized.get("specialization", "")) or "",
                normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
                teaching_languages,
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO teachers (name, email, phone, department, weekly_hours_limit, teaching_languages)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            normalized["name"],
            normalized["email"],
            normalized.get("phone", "") or "",
            normalized.get("department", normalized.get("specialization", "")) or "",
            normalized.get("weekly_hours_limit", normalized.get("max_hours_per_week")),
            teaching_languages,
        ),
    )
    return "inserted"


def _upsert_room(connection, payload):
    normalized = normalize_number_fields(payload, ["capacity", "computer_count"])
    normalized["type"] = _normalize_room_type(normalized.get("type"))
    normalized["available"] = _normalize_availability(normalized.get("available"))
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
            SET number = ?, capacity = ?, building = ?, type = ?, equipment = ?, department = ?, available = ?, computer_count = ?
            WHERE id = ?
            """,
            (
                str(normalized["number"]),
                normalized["capacity"],
                normalized.get("building", "") or "",
                normalized.get("type", "") or "",
                normalized.get("equipment", "") or "",
                normalized.get("department", "") or "",
                normalized.get("available", 1),
                normalized.get("computer_count", 0),
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO rooms (number, capacity, building, type, equipment, department, available, computer_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(normalized["number"]),
            normalized["capacity"],
            normalized.get("building", "") or "",
            normalized.get("type", "") or "",
            normalized.get("equipment", "") or "",
            normalized.get("department", "") or "",
            normalized.get("available", 1),
            normalized.get("computer_count", 0),
        ),
    )
    return "inserted"


def _normalize_bool(value):
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    normalized = _normalize_header(value).replace("_", " ")
    if normalized in {"1", "true", "yes", "да", "иә", "a/b", "a / b"}:
        return 1
    return 0


def _normalize_language(value, default="ru"):
    normalized = _normalize_header(value)
    if normalized in {"ru", "рус", "русский"}:
        return "ru"
    if normalized in {"kk", "kaz", "қаз", "каз", "kazakh", "қазақ", "казахский"}:
        return "kk"
    return default


def _normalize_teaching_languages(value):
    if value in (None, ""):
        return "ru,kk"
    values = str(value).replace(";", ",").split(",")
    result = []
    for item in values:
        normalized = _normalize_language(item, "")
        if normalized and normalized not in result:
            result.append(normalized)
    return ",".join(result or ["ru", "kk"])


def _upsert_group(connection, payload):
    normalized = normalize_number_fields(payload, ["student_count", "study_course"])
    has_subgroups = _normalize_bool(payload.get("has_subgroups"))
    language = _normalize_language(payload.get("language"), "ru")
    existing = query_one(
        connection,
        "SELECT id FROM groups WHERE lower(name) = lower(?)",
        (normalized["name"],),
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE groups
            SET name = ?, student_count = ?, study_course = ?, has_subgroups = ?, language = ?
            WHERE id = ?
            """,
            (
                normalized["name"],
                normalized["student_count"],
                normalized.get("study_course"),
                has_subgroups,
                language,
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO groups (name, student_count, study_course, has_subgroups, language)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            normalized["name"],
            normalized["student_count"],
            normalized.get("study_course"),
            has_subgroups,
            language,
        ),
    )
    return "inserted"


def _upsert_section(connection, payload):
    normalized = normalize_number_fields(payload, ["classes_count"])
    normalized["lesson_type"] = _normalize_lesson_type(normalized.get("lesson_type"))
    course = query_one(
        connection,
        """
        SELECT id, name, code
        FROM courses
        WHERE lower(code) = lower(?)
        """,
        (normalized["course_code"],),
    )
    if not course:
        raise ApiError(
            400,
            "bad_request",
            f"Для секции не найден курс с кодом '{normalized['course_code']}'.",
        )

    group = query_one(
        connection,
        """
        SELECT id, name
        FROM groups
        WHERE lower(name) = lower(?)
        """,
        (normalized["group_name"],),
    )
    if not group:
        raise ApiError(
            400,
            "bad_request",
            f"Для секции не найдена группа '{normalized['group_name']}'.",
        )

    existing = query_one(
        connection,
        "SELECT id FROM sections WHERE course_id = ? AND group_id = ?",
        (course["id"], group["id"]),
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE sections
            SET course_id = ?, course_name = ?, group_id = ?, group_name = ?, classes_count = ?, lesson_type = ?
            WHERE id = ?
            """,
            (
                course["id"],
                course["name"],
                group["id"],
                group["name"],
                normalized["classes_count"],
                normalized["lesson_type"],
                existing["id"],
            ),
        )
        return "updated"

    insert_and_get_id(
        connection,
        """
        INSERT INTO sections (course_id, course_name, group_id, group_name, classes_count, lesson_type)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            course["id"],
            course["name"],
            group["id"],
            group["name"],
            normalized["classes_count"],
            normalized["lesson_type"],
        ),
    )
    return "inserted"


def _load_rop_rows(file_name, file_bytes):
    lower_name = file_name.lower()
    if lower_name.endswith(".xls"):
        try:
            import xlrd
        except ImportError as exc:
            raise ApiError(
                500,
                "internal_server_error",
                "Excel .xls import dependency is not installed on the server.",
            ) from exc

        try:
            workbook = xlrd.open_workbook(file_contents=file_bytes)
        except Exception as exc:
            raise ApiError(400, "bad_request", "Не удалось прочитать РОП Excel файл.") from exc
        if not workbook.nsheets:
            raise ApiError(400, "bad_request", "В РОП Excel файле нет листов.")
        sheet = workbook.sheet_by_index(0)
        return [
            [_normalize_cell(sheet.cell_value(row_index, column_index)) for column_index in range(sheet.ncols)]
            for row_index in range(sheet.nrows)
        ]

    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ApiError(
            500,
            "internal_server_error",
            "Excel import dependency is not installed on the server.",
        ) from exc

    try:
        workbook = load_workbook(filename=BytesIO(file_bytes), data_only=True)
    except Exception as exc:
        raise ApiError(400, "bad_request", "Не удалось прочитать РОП Excel файл.") from exc
    sheet = workbook.worksheets[0]
    return [[_normalize_cell(cell) for cell in row] for row in sheet.iter_rows(values_only=True)]


def _extract_quoted_text(text):
    match = re.search(r"[“\"]([^“”\"]+)[”\"]", text)
    return match.group(1).strip() if match else ""


def _extract_academic_year(rows):
    for row in rows[:20]:
        text = " ".join(_cell_text(value) for value in row if _cell_text(value))
        match = re.search(r"(20\d{2})\s*[-–]\s*(20\d{2})", text)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
    return ""


def _extract_entry_year(rows):
    for row in rows[:20]:
        text = " ".join(_cell_text(value) for value in row if _cell_text(value))
        if "Год поступления" in text or "Оқуға түскен жылы" in text:
            return text.split(":", 1)[-1].strip()
    return ""


def _extract_programme_name(rows):
    for row in rows[:20]:
        text = " ".join(_cell_text(value) for value in row if _cell_text(value))
        quoted = _extract_quoted_text(text)
        if quoted and (
            "образовательной программы" in text.lower()
            or "білім беру бағдарламасының" in text.lower()
        ):
            return quoted
    return ""


def _extract_language(file_name, rows):
    lower_name = file_name.lower()
    if "каз" in lower_name or "_kk" in lower_name or "қаз" in lower_name:
        return "kk"
    if "рус" in lower_name or "_ru" in lower_name:
        return "ru"
    for row in rows[:20]:
        text = " ".join(_cell_text(value) for value in row if _cell_text(value))
        if any(marker in text for marker in ("ЖҰМЫС ОҚУ ЖОСПАРЫ", "Оқуға түскен жылы")):
            return "kk"
    return "ru"


def _extract_study_year(file_name, academic_periods):
    match = re.search(r"[_\-\s](\d)(?:[_\-\s]|$)", file_name)
    if match:
        return int(match.group(1))
    for academic_period in academic_periods:
        period_number = _extract_period_number(academic_period)
        if period_number:
            return int((period_number + 1) / 2)
    return None


def _extract_period_number(value):
    match = re.search(r"\d+", _cell_text(value))
    return int(match.group(0)) if match else None


def _semester_in_year(academic_period):
    if not academic_period:
        return None
    return 1 if academic_period % 2 == 1 else 2


def _weekly_classes_from_hours(hours):
    numeric_hours = _number_or_none(hours)
    if not numeric_hours:
        return 0
    return max(1, int(round(float(numeric_hours) / 15)))


def _find_rop_table_header_row(rows):
    for index, row in enumerate(rows):
        normalized_values = {_normalize_header(value) for value in row}
        if (
            "код дисциплины" in normalized_values
            or "пәннің коды" in normalized_values
            or "пәннің_коды" in normalized_values
        ):
            return index
    raise ApiError(400, "bad_request", "В РОП не найдена таблица дисциплин.")


def _is_rop_course_row(row):
    first_cell = _cell_text(_safe_row_value(row, 0)).lower()
    code = _cell_text(_safe_row_value(row, 5))
    name = _cell_text(_safe_row_value(row, 6))
    if not code or not name:
        return False
    if first_cell.startswith(("итого", "средняя", "барлығы", "оқу жоспары", "орташа")):
        return False
    return True


def parse_rop_preview(headers, payload):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    file_name, file_bytes = _decode_file_payload(
        payload,
        (".xls", ".xlsx"),
        "Поддерживаются Excel файлы РОП формата .xls или .xlsx.",
    )
    rows = _load_rop_rows(file_name, file_bytes)
    header_row_index = _find_rop_table_header_row(rows)
    academic_period_row = rows[header_row_index + 1] if header_row_index + 1 < len(rows) else []
    academic_periods = [
        _extract_period_number(_safe_row_value(academic_period_row, group["academic_period_column"]))
        for group in ROP_PERIOD_COLUMN_GROUPS
    ]

    study_year = _extract_study_year(file_name, academic_periods)
    metadata = {
        "fileName": file_name,
        "programme": _extract_programme_name(rows),
        "academicYear": _extract_academic_year(rows),
        "entryYear": _extract_entry_year(rows),
        "language": _extract_language(file_name, rows),
        "studyYear": study_year,
        "academicPeriods": [period for period in academic_periods if period],
    }

    courses = []
    offerings = []
    lesson_components = []
    seen_courses = set()

    for row_index, row in enumerate(rows[header_row_index + 3 :], start=header_row_index + 4):
        if not _is_rop_course_row(row):
            continue

        course = {
            "rowNumber": row_index,
            "moduleNumber": _cell_text(_safe_row_value(row, 0)),
            "moduleType": _cell_text(_safe_row_value(row, 1)),
            "moduleName": _cell_text(_safe_row_value(row, 2)),
            "cycle": _cell_text(_safe_row_value(row, 3)),
            "component": _cell_text(_safe_row_value(row, 4)),
            "code": _cell_text(_safe_row_value(row, 5)),
            "name": _cell_text(_safe_row_value(row, 6)),
            "credits": _number_or_none(_safe_row_value(row, 7)),
            "examPeriod": _number_or_none(_safe_row_value(row, 9)),
            "programme": metadata["programme"],
            "studyYear": study_year,
            "language": metadata["language"],
            "academicYear": metadata["academicYear"],
            "entryYear": metadata["entryYear"],
        }
        course_key = (course["code"].lower(), course["name"].lower())
        if course_key not in seen_courses:
            seen_courses.add(course_key)
            courses.append(course)

        for group, academic_period in zip(ROP_PERIOD_COLUMN_GROUPS, academic_periods):
            total_hours = _number_or_none(_safe_row_value(row, group["total"]))
            if not academic_period or not total_hours:
                continue

            offering = {
                "courseCode": course["code"],
                "courseName": course["name"],
                "academicPeriod": academic_period,
                "semester": _semester_in_year(academic_period),
                "studyYear": study_year,
                "totalHours": total_hours,
                "lectureHours": _number_or_none(_safe_row_value(row, group["lecture"])) or 0,
                "practicalHours": _number_or_none(_safe_row_value(row, group["practical"])) or 0,
                "labHours": _number_or_none(_safe_row_value(row, group["lab"])) or 0,
                "studioHours": _number_or_none(_safe_row_value(row, group["studio"])) or 0,
                "practiceHours": _number_or_none(_safe_row_value(row, group["practice"])) or 0,
                "sropHours": _number_or_none(_safe_row_value(row, group["srop"])) or 0,
                "sroHours": _number_or_none(_safe_row_value(row, group["sro"])) or 0,
            }
            offerings.append(offering)

            for lesson_type in ROP_LESSON_TYPES:
                hours = offering[f"{lesson_type}Hours"]
                if hours:
                    normalized_lesson_type = "practical" if lesson_type == "studio" else lesson_type
                    lesson_components.append(
                        {
                            "courseCode": course["code"],
                            "courseName": course["name"],
                            "programme": metadata["programme"],
                            "studyYear": study_year,
                            "academicPeriod": academic_period,
                            "semester": offering["semester"],
                            "lessonType": normalized_lesson_type,
                            "hours": hours,
                            "weeklyClasses": _weekly_classes_from_hours(hours),
                            "requiresComputers": normalized_lesson_type in ROP_PC_REQUIRED_LESSON_TYPES,
                        }
                    )

    if not courses:
        raise ApiError(400, "bad_request", "В РОП не найдены дисциплины.")

    return {
        "message": "ROP preview parsed successfully.",
        "metadata": metadata,
        "totals": {
            "courses": len(courses),
            "offerings": len(offerings),
            "lessonComponents": len(lesson_components),
        },
        "courses": courses,
        "offerings": offerings,
        "lessonComponents": lesson_components,
    }


def import_rop_data(headers, payload):
    preview = parse_rop_preview(headers, payload)
    course_by_key = {
        (course["code"], course["name"]): course for course in preview["courses"]
    }
    summary = {
        "courses": {"inserted": 0, "updated": 0},
        "courseComponents": {"inserted": 0},
    }

    with DB_LOCK:
        with get_connection() as connection:
            for offering in preview["offerings"]:
                course = course_by_key.get((offering["courseCode"], offering["courseName"]))
                if not course:
                    continue
                result, course_id = _upsert_rop_course(connection, course, offering)
                summary["courses"][result] += 1
                summary["courseComponents"]["inserted"] += _replace_rop_course_components(
                    connection,
                    course_id,
                    course,
                    offering,
                    preview["lessonComponents"],
                )
            connection.commit()

    return {
        "message": "ROP import completed successfully.",
        "metadata": preview["metadata"],
        "summary": summary,
        "totals": {
            "inserted": summary["courses"]["inserted"],
            "updated": summary["courses"]["updated"],
            "courseComponents": summary["courseComponents"]["inserted"],
            "courses": preview["totals"]["courses"],
            "offerings": preview["totals"]["offerings"],
            "lessonComponents": preview["totals"]["lessonComponents"],
        },
    }


def import_excel_data(headers, payload):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    workbook = _load_workbook(_decode_excel_payload(payload))
    sheet_map = {
        "courses": COURSE_HEADERS,
        "teachers": TEACHER_HEADERS,
        "rooms": ROOM_HEADERS,
        "groups": GROUP_HEADERS,
        "sections": SECTION_HEADERS,
    }
    recognized_sheets = []
    parsed_sheets = {}
    summary = {
        "courses": {"inserted": 0, "updated": 0},
        "teachers": {"inserted": 0, "updated": 0},
        "rooms": {"inserted": 0, "updated": 0},
        "groups": {"inserted": 0, "updated": 0},
        "sections": {"inserted": 0, "updated": 0},
    }

    with DB_LOCK:
        with get_connection() as connection:
            for sheet in workbook.worksheets:
                entity_name = _normalize_sheet_name(sheet.title)
                if not entity_name:
                    continue

                recognized_sheets.append(sheet.title)
                parsed_sheets[entity_name] = _read_sheet_rows(sheet, sheet_map[entity_name])

            # Teachers must be imported before courses so instructor_name can resolve to instructor_id.
            for entity_name in ("teachers", "courses", "rooms", "groups", "sections"):
                rows = parsed_sheets.get(entity_name, [])
                for row_index, row_payload in rows:
                    _validate_required_fields(entity_name, row_index, row_payload)
                    if entity_name == "courses":
                        result = _upsert_course(connection, row_payload)
                    elif entity_name == "teachers":
                        result = _upsert_teacher(connection, row_payload)
                    elif entity_name == "rooms":
                        result = _upsert_room(connection, row_payload)
                    elif entity_name == "groups":
                        result = _upsert_group(connection, row_payload)
                    else:
                        result = _upsert_section(connection, row_payload)
                    summary[entity_name][result] += 1

            if not recognized_sheets:
                raise ApiError(
                    400,
                    "bad_request",
                    "В Excel не найдены листы Disciplines, Teachers, Rooms, Groups или Sections.",
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


def generate_import_template(headers):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise ApiError(
            500,
            "internal_server_error",
            "Excel import dependency is not installed on the server.",
        ) from exc

    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    for sheet_name, headers_row in TEMPLATE_HEADERS.items():
        sheet = workbook.create_sheet(title=sheet_name)
        sheet.append(headers_row)
        for row in TEMPLATE_ROWS[sheet_name]:
            sheet.append(row)

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def generate_schedule_export(headers):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise ApiError(
            500,
            "internal_server_error",
            "Excel export dependency is not installed on the server.",
        ) from exc

    with DB_LOCK:
        with get_connection() as connection:
            schedules = query_all(
                connection,
                """
                SELECT
                    course_name,
                    group_name,
                    subgroup,
                    teacher_name,
                    room_number,
                    day,
                    start_hour,
                    semester,
                    year,
                    algorithm
                FROM schedules
                ORDER BY day, start_hour, course_name, group_name, id
                """,
            )

    if not schedules:
        raise ApiError(400, "bad_request", "Расписание ещё не сгенерировано.")

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Schedule"
    sheet.append(
        [
            "course_name",
            "group_name",
            "subgroup",
            "teacher_name",
            "room_number",
            "day",
            "start_hour",
            "semester",
            "year",
            "algorithm",
        ]
    )

    for item in schedules:
        sheet.append(
            [
                item.get("course_name", ""),
                item.get("group_name", ""),
                item.get("subgroup", ""),
                item.get("teacher_name", ""),
                item.get("room_number", ""),
                item.get("day", ""),
                item.get("start_hour", ""),
                item.get("semester", ""),
                item.get("year", ""),
                item.get("algorithm", ""),
            ]
        )

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
