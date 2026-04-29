from pathlib import Path

from backend.app.import_service import (
    _extract_iup_metadata,
    _missing_first_year_base_courses,
    _missing_courses_for_study_year,
    _normalise_iup_language,
    _normalise_iup_programme,
    _parse_iup_file,
    _normalize_rop_programme_name,
)
from backend.app.optimizer import HOURS_DEFAULT
from backend.app.preference_service import VALID_HOURS
from backend.app.section_generation import _is_first_year_base_iup_entry, _same_academic_programme, _same_study_course
from backend.app.section_generation import _build_component_index, _match_component
from backend.app.scheduling import _is_physical_education
from backend.app.time_slots import format_lesson_time_range


def test_rop_programme_uses_direction_from_file_name():
    assert _normalize_rop_programme_name("РОП_БИ_рус.xlsx", "Компьютерная инженерия") == "Бизнес-информатика"
    assert _normalize_rop_programme_name("РОП_КИ_каз.xlsx", "Бизнес-информатика") == "Компьютерная инженерия"
    assert _normalize_rop_programme_name("РОП_КИ_СОПР.xlsx", "Компьютерная инженерия") == "Компьютерная инженерия (СОПР)"


def test_iup_metadata_detects_programme_language_and_shortened_study():
    metadata = _extract_iup_metadata(
        "ИУП_05-057-23-01.pdf",
        [
            "Наименование группы образовательной программы (шифр) Компьютерная инженерия (6B06103)",
            "Форма обучения 3 года",
            "Оқыту тілі қазақ",
        ],
    )

    assert metadata["programme"] == "Компьютерная инженерия (СОПР)"
    assert metadata["language"] == "kk"
    assert metadata["groupName"] == "05-057-23-01 СОПР"


def test_iup_language_normalization_accepts_codes_and_labels():
    assert _normalise_iup_language("kk") == "kk"
    assert _normalise_iup_language("қазақ") == "kk"
    assert _normalise_iup_language("ru") == "ru"
    assert _normalise_iup_language("русский") == "ru"


def test_iup_programme_keeps_business_and_regular_computer_engineering_separate():
    assert _normalise_iup_programme("Бизнес-информатика (6B06102)") == "Бизнес-информатика"
    assert _normalise_iup_programme("Бизнес-информатика (6B06102), 3 года") == "Бизнес-информатика"
    assert _normalise_iup_programme("Компьютерная инженерия (6B06103), 4 года") == "Компьютерная инженерия"
    assert _normalise_iup_programme("Компьютерная инженерия (6B06103), 3 года") == "Компьютерная инженерия (СОПР)"


def test_group_programme_matching_distinguishes_b057_directions():
    bi_group = {"programme": "B057", "specialty_code": "6B06102", "name": "05-057-24-01"}
    ki_group = {"programme": "B057", "specialty_code": "6B06103", "name": "05-057-24-02"}
    sopr_group = {"programme": "B057", "specialty_code": "6B06103", "name": "05-057-23-01 СОПР"}
    bi_alias_group = {"programme": "B057", "specialty_code": "БИ", "name": "BI-24-01"}

    bi_iup = {"programme": "Бизнес-информатика", "group_name": "05-057-24-01"}
    ki_iup = {"programme": "Компьютерная инженерия", "group_name": "05-057-24-02"}
    sopr_iup = {"programme": "Компьютерная инженерия (СОПР)", "group_name": "05-057-23-01 СОПР"}

    assert _same_academic_programme(bi_group, bi_iup)
    assert _same_academic_programme(bi_alias_group, bi_iup)
    assert _same_academic_programme(ki_group, ki_iup)
    assert _same_academic_programme(sopr_group, sopr_iup)
    assert not _same_academic_programme(bi_group, ki_iup)
    assert not _same_academic_programme(ki_group, sopr_iup)


def test_iup_entry_must_match_group_current_study_course():
    group = {"study_course": 4}

    assert _same_study_course(group, {"study_course": 4})
    assert not _same_study_course(group, {"study_course": 1})
    assert _same_study_course(group, {"study_course": None})


def test_only_first_year_period_one_two_iup_entries_are_allowed_without_rop():
    assert _is_first_year_base_iup_entry({"study_course": 1, "academic_period": 1})
    assert _is_first_year_base_iup_entry({"study_course": 1, "academic_period": 2})
    assert not _is_first_year_base_iup_entry({"study_course": 1, "academic_period": 3})
    assert not _is_first_year_base_iup_entry({"study_course": 2, "academic_period": 1})


def test_only_first_year_missing_courses_are_auto_created_from_iup():
    missing_courses = [
        {"code": "Base 1101", "studyYear": 1},
        {"code": "Major 2201", "studyYear": 2},
        {"code": "Unknown"},
    ]

    first_year_courses = _missing_courses_for_study_year(missing_courses, 1)

    assert first_year_courses == [{"code": "Base 1101", "studyYear": 1}]


def test_only_first_year_period_one_two_missing_courses_are_auto_created_from_iup():
    missing_courses = [
        {"code": "Base 1101", "studyYear": 1, "semester": 1},
        {"code": "Base 1201", "studyYear": 1, "semester": 2},
        {"code": "Unexpected 1301", "studyYear": 1, "semester": 3},
        {"code": "Major 2101", "studyYear": 2, "semester": 1},
    ]

    base_courses = _missing_first_year_base_courses(missing_courses)

    assert base_courses == [
        {"code": "Base 1101", "studyYear": 1, "semester": 1},
        {"code": "Base 1201", "studyYear": 1, "semester": 2},
    ]


def test_rop_component_matching_ignores_rop_language():
    components = [
        {
            "component_id": 1,
            "course_id": 10,
            "course_code": "Alg 1201",
            "course_name": "Алгоритмы",
            "academic_period": 1,
            "lesson_type": "lecture",
            "language": "ru",
            "programme": "Бизнес-информатика",
        }
    ]
    iup_entry = {
        "course_code": "Alg 1201",
        "course_name": "Алгоритмы",
        "academic_period": 1,
        "lesson_type": "lecture",
        "language": "kk",
        "programme": "Бизнес-информатика",
    }

    component_index, by_lesson_period = _build_component_index(components)
    component, method = _match_component(iup_entry, component_index, by_lesson_period, [], True)

    assert component["course_id"] == 10
    assert method == "code"


def test_iup_parser_preserves_academic_period_boundaries_for_real_pdf():
    repo_root = Path(__file__).resolve().parents[2]
    iup_file = repo_root / "ИУП_Бизнес-информатика" / "25-19.pdf"
    if not iup_file.exists():
        return

    parsed = _parse_iup_file(iup_file.name, iup_file.read_bytes())
    periods = {entry["academicPeriod"] for entry in parsed["entries"]}

    assert {1, 2}.issubset(periods)


def test_physics_is_not_treated_as_physical_education():
    assert not _is_physical_education({"course_name": "Физика", "course_code": "Fiz 2242"})
    assert _is_physical_education({"course_name": "Физическая культура.", "course_code": "FK 1109"})


def test_schedule_time_slots_run_from_eight_to_nineteen():
    assert HOURS_DEFAULT[0] == 8
    assert HOURS_DEFAULT[-1] == 19
    assert len(HOURS_DEFAULT) == 12
    assert 19 in VALID_HOURS
    assert 20 not in VALID_HOURS
    assert format_lesson_time_range(8) == "08:00-08:50"
    assert format_lesson_time_range(19) == "19:00-19:50"
