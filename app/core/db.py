import json

from sqlalchemy import func, select

from .config import (
    DATA_DIR,
    DB_ENGINE,
    LEGACY_JSON_FILE,
)
from .orm import SessionLocal
from .store import default_store
from ..auth.security import hash_password
from ..models import (
    Course,
    Group,
    Room,
    Schedule,
    Section,
    Student,
    Teacher,
    User,
)
from ..teachers.utils import build_teacher_name_signature, normalize_teacher_name
from ..teachers.transliteration import teacher_name_translations
from ..courses.translations import course_meta_translations, discipline_name_translations


def _seed_user(row):
    return User(
        email=row["email"],
        password=hash_password(row["password"]),
        full_name=row.get("displayName") or row.get("full_name") or row["email"],
        role=row["role"],
        token=row["token"],
        avatar_data=row.get("avatarData") or row.get("avatar_data"),
        department=row.get("department", ""),
        programme=row.get("programmeName", row.get("programme", "")),
        group_id=row.get("group_id"),
        group_name=row.get("group_name", ""),
        subgroup=row.get("subgroup", ""),
    )


def _seed_course(row):
    name_i18n = discipline_name_translations(row["name"])
    programme_i18n = course_meta_translations(row.get("programme_name", row.get("programme", "")))
    cycle_i18n = course_meta_translations(row.get("cycle", ""))
    component_i18n = course_meta_translations(row.get("component", ""))
    department_i18n = course_meta_translations(row.get("department", ""))
    return Course(
        name=row["name"],
        name_kk=row.get("name_kk") or name_i18n["kk"],
        name_en=row.get("name_en") or name_i18n["en"],
        code=row["code"],
        credits=row.get("credits"),
        hours=row.get("hours"),
        description=row.get("description", ""),
        year=row.get("study_year", row.get("year")),
        semester=row.get("semester"),
        department=row.get("department", ""),
        instructor_id=row.get("instructor_id"),
        instructor_name=row.get("instructor_name", ""),
        programme=row.get("programme_name", row.get("programme", "")),
        programme_kk=row.get("programme_kk") or programme_i18n["kk"],
        programme_en=row.get("programme_en") or programme_i18n["en"],
        module_type=row.get("module_type", ""),
        module_name=row.get("module_name", ""),
        cycle=row.get("cycle", ""),
        cycle_kk=row.get("cycle_kk") or cycle_i18n["kk"],
        cycle_en=row.get("cycle_en") or cycle_i18n["en"],
        component=row.get("component", ""),
        component_kk=row.get("component_kk") or component_i18n["kk"],
        component_en=row.get("component_en") or component_i18n["en"],
        department_kk=row.get("department_kk") or department_i18n["kk"],
        department_en=row.get("department_en") or department_i18n["en"],
        language=row.get("language", ""),
        academic_year=row.get("academic_year", ""),
        entry_year=row.get("entry_year", ""),
        requires_computers=1 if row.get("requires_computers") else 0,
    )


def _seed_teacher(row):
    name_i18n = teacher_name_translations(
        row["name"],
        row.get("name_kk"),
        row.get("name_en"),
    )
    return Teacher(
        name=row["name"],
        name_kk=name_i18n["kk"],
        name_en=name_i18n["en"],
        email=row["email"],
        phone=row.get("phone", ""),
        department=row.get("department", ""),
        subject_taught=row.get("specialization", row.get("department", "")),
        weekly_hours_limit=row.get("max_hours_per_week", row.get("weekly_hours_limit")),
        name_normalized=normalize_teacher_name(row["name"]),
        name_signature=build_teacher_name_signature(row["name"]),
        teaching_languages=row.get("teaching_languages", "ru,kk"),
    )


def _seed_room(row):
    return Room(
        number=row["number"],
        capacity=row.get("capacity"),
        type=row.get("type", ""),
        building=row.get("building", ""),
        equipment=row.get("equipment", ""),
        programme=row.get("programme", row.get("department", "")),
        available=1 if row.get("is_available", row.get("available", 1)) else 0,
        computer_count=row.get("computer_count", 0),
    )


def _seed_group(row):
    return Group(
        name=row.get("name"),
        student_count=row.get("student_count") or 0,
        has_subgroups=row.get("has_subgroups", 0),
        language=row.get("language", "ru"),
        programme=row.get("programme", ""),
        specialty_code=row.get("specialty_code", ""),
        entry_year=row.get("entry_year"),
        study_course=row.get("study_course"),
    )


def _seed_schedule(row):
    return Schedule(
        course_id=row.get("course_id"),
        course_name=row.get("course_name") or "",
        teacher_id=row.get("teacher_id"),
        teacher_name=row.get("teacher_name") or "",
        room_id=row.get("room_id"),
        room_number=row.get("room_number") or "",
        day=row.get("day") or "",
        start_hour=row.get("start_hour") or 0,
        semester=row.get("semester"),
        year=row.get("year"),
        algorithm=row.get("algorithm"),
    )


def _seed_section(row):
    return Section(
        course_id=row.get("course_id"),
        course_name=row.get("course_name") or "",
        group_id=row.get("group_id"),
        group_name=row.get("group_name", ""),
        classes_count=row.get("class_count", row.get("classes_count")) or 1,
        lesson_type=row.get("lesson_type", "lecture"),
    )


def seed_from_store(store):
    with SessionLocal() as session:
        try:
            session.add_all(_seed_user(row) for row in store.get("users", []))
            session.add_all(_seed_course(row) for row in store.get("courses", []))
            session.add_all(_seed_teacher(row) for row in store.get("teachers", []))
            session.add_all(_seed_room(row) for row in store.get("rooms", []))
            session.add_all(_seed_group(row) for row in store.get("groups", []))
            session.add_all(_seed_schedule(row) for row in store.get("schedules", []))
            session.add_all(_seed_section(row) for row in store.get("sections", []))
            session.commit()
        except Exception:
            session.rollback()
            raise


def migrate_legacy_json():
    if not LEGACY_JSON_FILE.exists():
        return False

    with LEGACY_JSON_FILE.open("r", encoding="utf-8") as fh:
        store = json.load(fh)

    seed_from_store(store)
    LEGACY_JSON_FILE.rename(DATA_DIR / "store.migrated.json")
    return True


def _application_row_count(session):
    models = (User, Course, Teacher, Student, Room, Group, Schedule, Section)
    return sum(
        int(session.scalar(select(func.count()).select_from(model)) or 0)
        for model in models
    )


def ensure_database():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    from .migrations import run_startup_migrations

    run_startup_migrations()

    with SessionLocal() as session:
        has_application_data = _application_row_count(session) > 0
    if has_application_data:
        return

    if DB_ENGINE == "sqlite" and migrate_legacy_json():
        return

    seed_from_store(default_store())
