import re
from copy import deepcopy
from datetime import date

from ..core.config import TEACHER_EMAIL_DOMAIN
from ..core.errors import ApiError
from ..sections.lesson_rules import requires_computers_for_component

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

SPECIALTY_PROGRAMME_ALIASES = {
    "би": "Бизнес-информатика",
    "бизи": "Бизнес-информатика",
    "ки": "Компьютерная инженерия",
    "ки сопр": "Компьютерная инженерия (СОПР)",
    "сопр": "Компьютерная инженерия (СОПР)",
}
MIN_COMPUTER_COUNT = 10
PHYSICAL_EDUCATION_ROOM_NUMBER = "орленок"


def normalize_number_fields(payload, fields):
    normalized = deepcopy(payload)
    for field in fields:
        if field in normalized and normalized[field] not in ("", None):
            try:
                normalized[field] = int(normalized[field])
            except (TypeError, ValueError):
                pass
    return normalized


def positive_int(value, default=1):
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def normalize_room_block_interval(payload):
    normalized = normalize_number_fields(payload, ["room_id", "start_hour", "end_hour", "semester", "year"])
    day = str(normalized.get("day") or "").strip()
    start_hour = normalized.get("start_hour")
    end_hour = normalized.get("end_hour")
    if not normalized.get("room_id") or not day or start_hour in (None, ""):
        raise ApiError(400, "fill_required_fields", "Заполните поля: room_id, day, start_hour")
    if end_hour in (None, ""):
        end_hour = int(start_hour) + 1
    if int(end_hour) <= int(start_hour):
        raise ApiError(400, "bad_request", "В блокировке аудитории end_hour должен быть больше start_hour")
    normalized["day"] = day
    normalized["start_hour"] = int(start_hour)
    normalized["end_hour"] = int(end_hour)
    return normalized


def normalize_lesson_type(value):
    if value in (None, ""):
        return "lecture"
    normalized = str(value).strip().lower().replace("_", " ")
    compact = normalized.replace(" ", "_")
    return LESSON_TYPE_ALIASES.get(compact, LESSON_TYPE_ALIASES.get(normalized, str(value).strip().lower()))


def normalize_subgroup_mode(value, lesson_type="lecture"):
    if lesson_type == "lecture":
        return "none"
    normalized = str(value or "auto").strip().lower()
    return normalized if normalized in {"none", "auto", "forced"} else "auto"


def section_requires_computers(lesson_type, course_code="", course_name="", study_year=None):
    return 1 if requires_computers_for_component(lesson_type, course_code, course_name, study_year) else 0


def is_physical_education_course(course_name="", course_code=""):
    text = f"{course_name or ''} {course_code or ''}".strip().lower()
    return (
        "физическая культура" in text
        or "дене шынықтыру" in text
        or "fk " in f"{text} "
    )


def is_physical_education_room(room_number):
    return PHYSICAL_EDUCATION_ROOM_NUMBER in str(room_number or "").strip().lower()


def validate_teacher_email(email):
    normalized_email = (email or "").strip().lower()
    if not normalized_email.endswith(TEACHER_EMAIL_DOMAIN):
        raise ApiError(
            400,
            "teacher_email_domain_required",
            "Для преподавателя нужен email, оканчивающийся на @kazatu.edu.kz",
        )


def normalize_language(value, default="ru"):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"ru", "kk"} else default


def normalize_teaching_languages(value):
    raw_values = value.split(",") if isinstance(value, str) else (value or [])
    result = []
    for raw in raw_values:
        normalized = normalize_language(raw, "")
        if normalized and normalized not in result:
            result.append(normalized)
    return result or ["ru", "kk"]


def normalize_specialty(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return normalized.upper() if normalized else ""


def normalize_programme(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return SPECIALTY_PROGRAMME_ALIASES.get(normalized, str(value or "").strip())


def infer_group_entry_year(group_name):
    match = re.search(r"\b05-057-(\d{2})-", str(group_name or ""))
    if not match:
        return None
    year_suffix = int(match.group(1))
    return 2000 + year_suffix


def current_academic_year_start():
    today = date.today()
    return today.year if today.month >= 9 else today.year - 1


def infer_study_course(entry_year):
    if not entry_year:
        return None
    return max(1, current_academic_year_start() - int(entry_year) + 1)


def normalize_subgroup(value):
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"A", "B"} else ""


def normalize_room_type(value):
    return str(value or "").strip().lower()


def schedule_room_type_matches(room_type, lesson_type, requires_computers=False):
    normalized_room_type = normalize_room_type(room_type)
    normalized_lesson_type = normalize_lesson_type(lesson_type)
    if normalized_lesson_type == "lecture":
        return normalized_room_type == "lecture"
    if normalized_lesson_type == "practical":
        return normalized_room_type in {"practical", "lecture"}
    if normalized_lesson_type == "lab":
        return normalized_room_type == "practical"
    return normalized_room_type == "practical"
