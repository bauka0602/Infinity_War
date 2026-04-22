IT_PRACTICAL_KEYWORDS = (
    "алгоритм",
    "ақпарат",
    "бағдарлам",
    "баз",
    "веб",
    "данн",
    "жасанды",
    "жел",
    "информацион",
    "информатика",
    "кибер",
    "компьютер",
    "мобиль",
    "нейрон",
    "облач",
    "программ",
    "разработка",
    "сети",
    "систем",
    "цифр",
    "ict",
    "ikt",
    "java",
    "machine",
    "python",
    "security",
    "software",
    "sql",
    "web",
)


def _combined_course_text(course_code="", course_name=""):
    return f"{course_code or ''} {course_name or ''}".strip().lower()


def _study_year_or_none(study_year):
    try:
        return int(study_year)
    except (TypeError, ValueError):
        return None


def is_it_practical_course(course_code="", course_name=""):
    text = _combined_course_text(course_code, course_name)
    return any(keyword in text for keyword in IT_PRACTICAL_KEYWORDS)


def requires_computers_for_component(lesson_type, course_code="", course_name="", study_year=None):
    normalized_type = str(lesson_type or "").strip().lower()
    if normalized_type == "lab":
        return True
    if normalized_type != "practical":
        return False

    year = _study_year_or_none(study_year)
    if year is not None and year <= 1:
        return False

    return is_it_practical_course(course_code, course_name)
