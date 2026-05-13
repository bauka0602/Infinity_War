import hashlib
import logging
import re
from difflib import SequenceMatcher

from sqlalchemy import func, or_, select

from ..collections.normalization import normalize_lesson_type, normalize_language, positive_int
from ..courses.translations import course_meta_translations, discipline_name_translations
from ..core.errors import ApiError
from ..core.orm import SessionLocal
from ..models import Course, CourseComponent, Group, IupEntry, Room, Section, Teacher
from .lesson_rules import requires_computers_for_component
from ..programmes.utils import normalize_programme_text, same_programme
from ..teachers.utils import build_teacher_name_signature, normalize_teacher_name

LOGGER = logging.getLogger(__name__)

ACTIVE_LESSON_TYPES = {"lecture", "practical", "lab"}
ELECTIVE_COMPONENTS = {"кв", "дво"}
MAX_ELECTIVES_PER_GROUP = 12


def normalize_bool(value, default=False):
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_course_code(value):
    return re.sub(r"[^a-zа-я0-9]", "", str(value or "").lower().replace("ё", "е"))


def normalize_course_name(value):
    normalized = str(value or "").lower().replace("ё", "е")
    normalized = re.sub(r"[^a-zа-я0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_component(value):
    return str(value or "").strip().upper()


def add_issue(issues, code, message, severity="warning", **details):
    issue = {"code": code, "severity": severity, "message": message}
    issue.update({key: value for key, value in details.items() if value not in (None, "")})
    issues.append(issue)
    return issue


def _period_matches(left, right):
    if left in (None, "") or right in (None, ""):
        return False
    return int(left) == int(right)


def _same_language(left, right):
    return normalize_language(left, "") == normalize_language(right, "")


def _same_programme(left, right):
    left_normalized = normalize_programme_text(left)
    right_normalized = normalize_programme_text(right)
    if not left_normalized or not right_normalized:
        return False
    return same_programme(left_normalized, right_normalized)


def _programme_kind(programme="", specialty_code="", group_name=""):
    text = " ".join(
        str(value or "").lower()
        for value in (programme, specialty_code, group_name)
    )
    if "сопр" in text:
        return "ki_sopr"
    if "6b06102" in text or "бизнес" in text or re.search(r"(^|[^a-zа-я])(?:би|bi)([^a-zа-я]|$)", text):
        return "bi"
    if "6b06103" in text or "компьютер" in text or re.search(r"(^|[^a-zа-я])(?:ки|ki)([^a-zа-я]|$)", text):
        return "ki"
    return ""


def _same_academic_programme(group, iup_entry):
    group_kind = _programme_kind(
        group.get("programme"),
        group.get("specialty_code"),
        group.get("name"),
    )
    iup_kind = _programme_kind(iup_entry.get("programme"), "", iup_entry.get("group_name"))
    if group_kind and iup_kind:
        return group_kind == iup_kind
    if str(group.get("programme") or "").strip().lower() == "b057":
        return bool(iup_kind)
    return _same_programme(group.get("programme"), iup_entry.get("programme"))


def _same_study_course(group, iup_entry):
    group_course = group.get("study_course")
    entry_course = iup_entry.get("study_course")
    if group_course in (None, "") or entry_course in (None, ""):
        return True
    return int(group_course) == int(entry_course)


def _is_first_year_base_iup_entry(iup_entry):
    return int(iup_entry.get("study_course") or 0) == 1 and int(iup_entry.get("academic_period") or 0) in {1, 2}


def _classes_count(iup_entry, component):
    weekly = component.get("weekly_classes")
    if weekly not in (None, ""):
        result = positive_int(weekly, 1)
    else:
        hours = iup_entry.get("hours") if iup_entry.get("hours") not in (None, "") else component.get("hours")
        try:
            result = round(float(hours or 0) / 15)
        except (TypeError, ValueError):
            result = 1
        result = positive_int(result, 1)
    return max(1, min(4, int(result)))


def _load_rop_components(session, payload):
    clauses = [CourseComponent.lesson_type.in_(("lecture", "practical", "lab"))]
    if payload.get("academic_period"):
        clauses.append(CourseComponent.academic_period == int(payload["academic_period"]))
    if payload.get("semester"):
        clauses.append(CourseComponent.semester == int(payload["semester"]))
    if payload.get("programme"):
        clauses.append(func.coalesce(CourseComponent.programme, Course.programme, "") != "")

    return session.execute(
        select(
            CourseComponent.id.label("component_id"),
            CourseComponent.course_id.label("course_id"),
            CourseComponent.course_code.label("course_code"),
            CourseComponent.course_name.label("course_name"),
            CourseComponent.programme.label("programme"),
            CourseComponent.study_year.label("study_year"),
            CourseComponent.academic_period.label("academic_period"),
            CourseComponent.semester.label("semester"),
            CourseComponent.lesson_type.label("lesson_type"),
            CourseComponent.hours.label("hours"),
            CourseComponent.weekly_classes.label("weekly_classes"),
            CourseComponent.requires_computers.label("requires_computers"),
            Course.language.label("language"),
            Course.component.label("component"),
            Course.programme.label("course_programme"),
        )
        .join(Course, Course.id == CourseComponent.course_id)
        .where(*clauses)
        .order_by(
            CourseComponent.academic_period,
            CourseComponent.course_name,
            CourseComponent.lesson_type,
            CourseComponent.id,
        )
    ).mappings().all()


def _load_iup_entries(session, payload):
    clauses = [IupEntry.lesson_type.in_(("lecture", "practical", "lab"))]
    field_map = {
        "group_name": IupEntry.group_name,
        "academic_period": IupEntry.academic_period,
        "semester": IupEntry.semester,
        "programme": IupEntry.programme,
        "language": IupEntry.language,
    }
    for field in ("group_name", "academic_period", "semester", "programme", "language"):
        value = payload.get(field)
        if value in (None, ""):
            continue
        column = field_map[field]
        if field in {"academic_period", "semester"}:
            clauses.append(column == int(value))
        else:
            clauses.append(func.lower(func.coalesce(column, "")) == func.lower(str(value).strip()))
    return session.execute(
        select(
            IupEntry.id.label("id"),
            IupEntry.file_name.label("file_name"),
            IupEntry.group_name.label("group_name"),
            IupEntry.programme.label("programme"),
            IupEntry.study_course.label("study_course"),
            IupEntry.language.label("language"),
            IupEntry.academic_year.label("academic_year"),
            IupEntry.academic_period.label("academic_period"),
            IupEntry.semester.label("semester"),
            IupEntry.component.label("component"),
            IupEntry.course_code.label("course_code"),
            IupEntry.course_name.label("course_name"),
            IupEntry.credits.label("credits"),
            IupEntry.lesson_type.label("lesson_type"),
            IupEntry.teacher_id.label("teacher_id"),
            IupEntry.teacher_name.label("teacher_name"),
            IupEntry.hours.label("hours"),
        )
        .where(*clauses)
        .order_by(
            IupEntry.group_name,
            IupEntry.academic_period,
            IupEntry.course_name,
            IupEntry.lesson_type,
            IupEntry.id,
        )
    ).mappings().all()


def _build_component_index(components):
    index = {}
    by_lesson_period = {}
    for component in components:
        key = (
            normalize_course_code(component.get("course_code")),
            int(component.get("academic_period") or 0),
            normalize_lesson_type(component.get("lesson_type")),
            normalize_programme_text(component.get("programme") or component.get("course_programme")),
        )
        index.setdefault(key, []).append(component)
        by_lesson_period.setdefault((key[1], key[2], key[3]), []).append(component)
    return index, by_lesson_period


def _match_component(iup_entry, component_index, by_lesson_period, issues, strict_mode):
    key = (
        normalize_course_code(iup_entry.get("course_code")),
        int(iup_entry.get("academic_period") or 0),
        normalize_lesson_type(iup_entry.get("lesson_type")),
        normalize_programme_text(iup_entry.get("programme")),
    )
    candidates = component_index.get(key) or []
    if candidates:
        return candidates[0], "code"

    fallback_candidates = by_lesson_period.get(key[1:], [])
    iup_name = normalize_course_name(iup_entry.get("course_name"))
    best = None
    best_score = 0.0
    for component in fallback_candidates:
        score = SequenceMatcher(None, iup_name, normalize_course_name(component.get("course_name"))).ratio()
        if score > best_score:
            best = component
            best_score = score
    if best and best_score >= 0.82:
        add_issue(
            issues,
            "fuzzy_match_used",
            "Дисциплина ИУП сопоставлена с РОП по названию.",
            "warning",
            iup_entry_id=iup_entry.get("id"),
            course_name=iup_entry.get("course_name"),
            matched_course_name=best.get("course_name"),
            score=round(best_score, 3),
        )
        return best, "fuzzy"

    add_issue(
        issues,
        "unmatched_iup",
        "Дисциплина есть в ИУП, но не найдена в РОП.",
        "error" if strict_mode else "warning",
        iup_entry_id=iup_entry.get("id"),
        group_name=iup_entry.get("group_name"),
        course_code=iup_entry.get("course_code"),
        course_name=iup_entry.get("course_name"),
    )
    return None, "unmatched"


def _find_group(session, iup_entry, issues):
    group_name = str(iup_entry.get("group_name") or "").strip()
    if not group_name:
        add_issue(issues, "group_missing", "В записи ИУП не указана группа.", "error", iup_entry_id=iup_entry.get("id"))
        return None
    group = session.execute(
        select(
            Group.id.label("id"),
            Group.name.label("name"),
            Group.student_count.label("student_count"),
            Group.language.label("language"),
            Group.programme.label("programme"),
            Group.specialty_code.label("specialty_code"),
            Group.study_course.label("study_course"),
        ).where(func.lower(Group.name) == func.lower(group_name))
    ).mappings().first()
    if not group:
        add_issue(issues, "group_missing", "Группа из ИУП не найдена в БД.", "error", group_name=group_name)
        return None
    if not _same_language(group.get("language"), iup_entry.get("language")):
        add_issue(issues, "language_mismatch", "Язык группы и дисциплины ИУП не совпадает.", "error", group_name=group_name)
        return None
    if not _same_academic_programme(group, iup_entry):
        add_issue(issues, "programme_mismatch", "Программа группы и дисциплины ИУП не совпадает.", "error", group_name=group_name)
        return None
    return group


def _teacher_to_dict(teacher):
    return {"id": teacher.id, "name": teacher.name} if teacher else None


def _find_or_create_teacher(session, iup_entry, issues):
    teacher_id = iup_entry.get("teacher_id")
    if teacher_id:
        teacher = session.get(Teacher, int(teacher_id))
        if teacher:
            return _teacher_to_dict(teacher)

    teacher_name = str(iup_entry.get("teacher_name") or "").strip()
    if not teacher_name:
        add_issue(
            issues,
            "teacher_missing",
            "В ИУП не указан преподаватель, section не создан.",
            "error",
            iup_entry_id=iup_entry.get("id"),
            course_name=iup_entry.get("course_name"),
        )
        return None

    signature = build_teacher_name_signature(teacher_name)
    teacher = session.scalar(
        select(Teacher)
        .where(Teacher.name_signature == signature)
        .order_by(Teacher.id)
        .limit(1)
    )
    if teacher:
        return _teacher_to_dict(teacher)

    digest = hashlib.sha1(normalize_teacher_name(teacher_name).encode("utf-8")).hexdigest()[:10]
    email = f"teacher-{digest}@kazatu.edu.kz"
    existing_email = session.scalar(
        select(Teacher)
        .where(func.lower(Teacher.email) == func.lower(email))
        .order_by(Teacher.id)
        .limit(1)
    )
    if existing_email:
        return _teacher_to_dict(existing_email)

    teacher = Teacher(
        name=teacher_name,
        email=email,
        phone="",
        subject_taught=iup_entry.get("course_name") or "",
        weekly_hours_limit=None,
        name_normalized=normalize_teacher_name(teacher_name),
        name_signature=signature,
        teaching_languages=normalize_language(iup_entry.get("language"), "ru"),
    )
    session.add(teacher)
    session.flush()
    add_issue(issues, "teacher_created", "Преподаватель из ИУП создан в БД.", "info", teacher_id=teacher.id, teacher_name=teacher_name)
    return _teacher_to_dict(teacher)


def _find_teacher_for_preview(session, iup_entry, issues):
    teacher_id = iup_entry.get("teacher_id")
    if teacher_id:
        teacher = session.get(Teacher, int(teacher_id))
        if teacher:
            return _teacher_to_dict(teacher)

    teacher_name = str(iup_entry.get("teacher_name") or "").strip()
    if not teacher_name:
        add_issue(
            issues,
            "teacher_missing",
            "В ИУП не указан преподаватель, section не будет создан.",
            "error",
            iup_entry_id=iup_entry.get("id"),
            course_name=iup_entry.get("course_name"),
        )
        return None

    signature = build_teacher_name_signature(teacher_name)
    teacher = session.scalar(
        select(Teacher)
        .where(Teacher.name_signature == signature)
        .order_by(Teacher.id)
        .limit(1)
    )
    if teacher:
        return _teacher_to_dict(teacher)

    add_issue(
        issues,
        "teacher_would_be_created",
        "Преподаватель из ИУП будет создан при генерации.",
        "info",
        iup_entry_id=iup_entry.get("id"),
        teacher_name=teacher_name,
    )
    return {"id": None, "name": teacher_name}


def _fallback_component_from_iup(session, iup_entry, preview=False):
    course = session.scalar(
        select(Course)
        .where(
            func.lower(Course.code) == func.lower(iup_entry.get("course_code") or iup_entry.get("course_name") or ""),
            Course.semester == iup_entry.get("academic_period"),
            func.lower(func.coalesce(Course.programme, "")) == func.lower(iup_entry.get("programme") or ""),
        )
        .order_by(Course.id)
        .limit(1)
    )
    if course:
        course_id = course.id
    elif preview:
        course_id = None
    else:
        name_i18n = discipline_name_translations(iup_entry.get("course_name"))
        programme_i18n = course_meta_translations(iup_entry.get("programme") or "")
        component_i18n = course_meta_translations(iup_entry.get("component") or "")
        course = Course(
            name=iup_entry.get("course_name"),
            name_kk=iup_entry.get("course_name_kk") or name_i18n["kk"],
            name_en=iup_entry.get("course_name_en") or name_i18n["en"],
            code=iup_entry.get("course_code") or iup_entry.get("course_name"),
            credits=iup_entry.get("credits"),
            hours=iup_entry.get("hours"),
            description="Fallback course created from IUP without ROP match.",
            year=iup_entry.get("study_course"),
            semester=iup_entry.get("academic_period"),
            department="",
            instructor_id=None,
            instructor_name="",
            programme=iup_entry.get("programme") or "",
            programme_kk=programme_i18n["kk"],
            programme_en=programme_i18n["en"],
            module_type="",
            module_name="",
            cycle="",
            cycle_kk="",
            cycle_en="",
            component=iup_entry.get("component") or "",
            component_kk=component_i18n["kk"],
            component_en=component_i18n["en"],
            department_kk="",
            department_en="",
            language=iup_entry.get("language") or "",
            academic_year=iup_entry.get("academic_year") or "",
            entry_year="",
            requires_computers=1 if requires_computers_for_component(
                iup_entry.get("lesson_type"),
                iup_entry.get("course_code"),
                iup_entry.get("course_name"),
                iup_entry.get("study_course"),
            ) else 0,
        )
        session.add(course)
        session.flush()
        course_id = course.id
    return {
        "component_id": None,
        "course_id": course_id,
        "course_code": iup_entry.get("course_code"),
        "course_name": iup_entry.get("course_name"),
        "programme": iup_entry.get("programme"),
        "study_year": iup_entry.get("study_course"),
        "academic_period": iup_entry.get("academic_period"),
        "semester": iup_entry.get("semester"),
        "lesson_type": normalize_lesson_type(iup_entry.get("lesson_type")),
        "hours": iup_entry.get("hours"),
        "weekly_classes": None,
        "requires_computers": requires_computers_for_component(
            iup_entry.get("lesson_type"),
            iup_entry.get("course_code"),
            iup_entry.get("course_name"),
            iup_entry.get("study_course"),
        ),
    }


def _build_sections_from_iup(connection, payload, preview=False):
    strict_mode = normalize_bool(payload.get("strict_mode", payload.get("strictMode")), True)
    issues = []
    with SessionLocal() as session:
        try:
            components = _load_rop_components(session, payload)
            iup_entries = _load_iup_entries(session, payload)
            component_index, by_lesson_period = _build_component_index(components)

            if not iup_entries:
                raise ApiError(400, "iup_entries_missing", "Нет записей ИУП для генерации sections.")
            has_first_year_iup_entries = any(
                normalize_lesson_type(entry.get("lesson_type")) in ACTIVE_LESSON_TYPES
                and _is_first_year_base_iup_entry(entry)
                for entry in iup_entries
            )
            if not components and strict_mode and not has_first_year_iup_entries:
                raise ApiError(400, "rop_components_missing", "Нет компонентов РОП для сопоставления с ИУП.")

            inserted = 0
            updated = 0
            skipped = 0
            generated_sections = []
            electives_by_group = {}

            for iup_entry in iup_entries:
                lesson_type = normalize_lesson_type(iup_entry.get("lesson_type"))
                if lesson_type not in ACTIVE_LESSON_TYPES:
                    skipped += 1
                    continue
                group = _find_group(session, iup_entry, issues)
                if not group:
                    skipped += 1
                    continue
                if not _same_study_course(group, iup_entry):
                    add_issue(
                        issues,
                        "study_course_mismatch",
                        "Строка ИУП относится к другому курсу обучения и не использована.",
                        "info",
                        iup_entry_id=iup_entry.get("id"),
                        group_name=group.get("name"),
                        group_study_course=group.get("study_course"),
                        iup_study_course=iup_entry.get("study_course"),
                        course_name=iup_entry.get("course_name"),
                    )
                    skipped += 1
                    continue
                if normalize_component(iup_entry.get("component")) in ELECTIVE_COMPONENTS:
                    electives_by_group[iup_entry.get("group_name")] = electives_by_group.get(iup_entry.get("group_name"), 0) + 1
                component, match_method = _match_component(iup_entry, component_index, by_lesson_period, issues, strict_mode)
                if component is None:
                    if strict_mode and not _is_first_year_base_iup_entry(iup_entry):
                        skipped += 1
                        continue
                    issue_code = "first_year_iup_course_used" if _is_first_year_base_iup_entry(iup_entry) else "fallback_iup_course_used"
                    issue_message = (
                        "Предмет 1 курса 1/2 периода создан по ИУП без РОП."
                        if _is_first_year_base_iup_entry(iup_entry)
                        else "Section будет создан по ИУП без найденного РОП."
                    )
                    add_issue(issues, issue_code, issue_message, "warning", iup_entry_id=iup_entry["id"])
                    component = _fallback_component_from_iup(session, iup_entry, preview=preview)
                    match_method = "first_year_iup" if _is_first_year_base_iup_entry(iup_entry) else "fallback_iup"

                teacher = _find_teacher_for_preview(session, iup_entry, issues) if preview else _find_or_create_teacher(session, iup_entry, issues)
                if not teacher:
                    skipped += 1
                    continue

                classes_count = _classes_count(iup_entry, component)
                if classes_count < 1 or classes_count > 4:
                    add_issue(issues, "classes_count_invalid", "classes_count скорректирован до диапазона 1..4.", "warning", iup_entry_id=iup_entry["id"])
                requires_computers = 1 if lesson_type == "lab" else 0
                section = {
                    "course_id": component["course_id"],
                    "course_name": component["course_name"],
                    "group_id": group["id"],
                    "group_name": group["name"],
                    "classes_count": classes_count,
                    "lesson_type": lesson_type,
                    "subgroup_mode": "none" if lesson_type == "lecture" else "auto",
                    "subgroup_count": 1,
                    "requires_computers": requires_computers,
                    "teacher_id": teacher["id"],
                    "teacher_name": teacher["name"],
                    "iup_entry_id": iup_entry["id"],
                    "source": "iup",
                    "match_method": match_method,
                }

                existing = session.scalar(
                    select(Section.id)
                    .where(
                        Section.course_id == section["course_id"],
                        Section.group_id == section["group_id"],
                        Section.lesson_type == section["lesson_type"],
                    )
                    .limit(1)
                )
                if preview:
                    action = "updated" if existing else "inserted"
                    section_id = existing if existing else None
                else:
                    section_id, action = _insert_or_update_section_in_session(session, section)
                    LOGGER.info("section_%s_from_iup section_id=%s iup_entry_id=%s", action, section_id, iup_entry["id"])

                inserted += 1 if action == "inserted" else 0
                updated += 1 if action == "updated" else 0
                generated_sections.append({"id": section_id, "action": action, **section})

            for group_name in {entry.get("group_name") for entry in iup_entries}:
                elective_count = electives_by_group.get(group_name, 0)
                if elective_count == 0:
                    add_issue(issues, "electives_missing", "У группы нет КВ в ИУП.", "warning", group_name=group_name)
                elif elective_count > MAX_ELECTIVES_PER_GROUP:
                    add_issue(issues, "electives_overflow", "В ИУП группы слишком много КВ.", "warning", group_name=group_name, count=elective_count)

            iup_keys = {
                (
                    normalize_course_code(entry.get("course_code")),
                    int(entry.get("academic_period") or 0),
                    normalize_lesson_type(entry.get("lesson_type")),
                    normalize_programme_text(entry.get("programme")),
                )
                for entry in iup_entries
            }
            for component in components:
                component_key = (
                    normalize_course_code(component.get("course_code")),
                    int(component.get("academic_period") or 0),
                    normalize_lesson_type(component.get("lesson_type")),
                    normalize_programme_text(component.get("programme") or component.get("course_programme")),
                )
                if component_key not in iup_keys:
                    add_issue(
                        issues,
                        "rop_without_iup",
                        "Дисциплина есть в РОП, но отсутствует в ИУП и не использована.",
                        "info",
                        course_code=component.get("course_code"),
                        course_name=component.get("course_name"),
                        academic_period=component.get("academic_period"),
                    )

            if not preview:
                session.commit()
        except Exception:
            session.rollback()
            raise

    return {
        "preview": preview,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "strict_mode": strict_mode,
        "issues": issues,
        "sections": generated_sections,
    }


def _insert_or_update_section_in_session(session, section):
    existing = session.scalar(
        select(Section)
        .where(
            Section.course_id == section["course_id"],
            Section.group_id == section["group_id"],
            Section.lesson_type == section["lesson_type"],
        )
        .limit(1)
    )
    if existing:
        original_teacher_id = existing.teacher_id
        existing.course_id = section["course_id"]
        existing.course_name = section["course_name"]
        existing.group_id = section["group_id"]
        existing.group_name = section["group_name"]
        existing.classes_count = section["classes_count"]
        existing.lesson_type = section["lesson_type"]
        existing.subgroup_mode = section["subgroup_mode"]
        existing.subgroup_count = section["subgroup_count"]
        existing.requires_computers = section["requires_computers"]
        if original_teacher_id is None:
            existing.teacher_id = section["teacher_id"]
        if original_teacher_id is None or not existing.teacher_name:
            existing.teacher_name = section["teacher_name"]
        existing.iup_entry_id = section["iup_entry_id"]
        existing.source = section["source"]
        existing.match_method = section["match_method"]
        session.flush()
        return existing.id, "updated"

    row = Section(
        course_id=section["course_id"],
        course_name=section["course_name"],
        group_id=section["group_id"],
        group_name=section["group_name"],
        classes_count=section["classes_count"],
        lesson_type=section["lesson_type"],
        subgroup_mode=section["subgroup_mode"],
        subgroup_count=section["subgroup_count"],
        requires_computers=section["requires_computers"],
        teacher_id=section["teacher_id"],
        teacher_name=section["teacher_name"],
        iup_entry_id=section["iup_entry_id"],
        source=section["source"],
        match_method=section["match_method"],
    )
    session.add(row)
    session.flush()
    return row.id, "inserted"


def _insert_or_update_section(connection, section):
    with SessionLocal() as session:
        section_id, action = _insert_or_update_section_in_session(session, section)
        session.commit()
        return section_id, action


def generate_sections_from_iup(connection, payload):
    return _build_sections_from_iup(connection, payload, preview=False)


def preview_sections_from_iup(connection, payload):
    return _build_sections_from_iup(connection, payload, preview=True)


def build_validation_report(connection):
    issues = []
    with SessionLocal() as session:
        sections = session.execute(
            select(
                Section.id.label("id"),
                Section.course_id.label("course_id"),
                Section.course_name.label("course_name"),
                Section.group_id.label("group_id"),
                Section.group_name.label("group_name"),
                Section.lesson_type.label("lesson_type"),
                Section.classes_count.label("classes_count"),
                Section.teacher_id.label("teacher_id"),
                Section.teacher_name.label("teacher_name"),
                Section.requires_computers.label("requires_computers"),
                Group.student_count.label("student_count"),
            )
            .outerjoin(Group, Group.id == Section.group_id)
            .order_by(Section.id)
        ).mappings().all()
        components = _load_rop_components(session, {})
        iup_entries = _load_iup_entries(session, {})
        rooms = session.execute(
            select(
                Room.id.label("id"),
                Room.number.label("number"),
                Room.capacity.label("capacity"),
                Room.type.label("type"),
                Room.computer_count.label("computer_count"),
                Room.available.label("available"),
            )
            .where(func.coalesce(Room.available, 1) == 1)
            .order_by(Room.id)
        ).mappings().all()
    if not sections:
        return {
            "summary": {
                "errors": 0,
                "warnings": 0,
                "info": 0,
                "sections": 0,
            },
            "issues": [],
        }

    component_index, _by_lesson_period = _build_component_index(components)
    iup_keys = set()
    for entry in iup_entries:
        key = (
            normalize_course_code(entry.get("course_code")),
            int(entry.get("academic_period") or 0),
            normalize_lesson_type(entry.get("lesson_type")),
            normalize_programme_text(entry.get("programme")),
        )
        iup_keys.add(key)
        if key not in component_index:
            add_issue(
                issues,
                "unmatched_iup",
                "Дисциплина есть в ИУП, но не найдена в РОП.",
                "warning",
                iup_entry_id=entry.get("id"),
                group_name=entry.get("group_name"),
                course_code=entry.get("course_code"),
                course_name=entry.get("course_name"),
            )
    for component in components:
        key = (
            normalize_course_code(component.get("course_code")),
            int(component.get("academic_period") or 0),
            normalize_lesson_type(component.get("lesson_type")),
            normalize_programme_text(component.get("programme") or component.get("course_programme")),
        )
        if key not in iup_keys:
            add_issue(
                issues,
                "rop_without_iup",
                "Дисциплина есть в РОП, но отсутствует в ИУП.",
                "info",
                course_code=component.get("course_code"),
                course_name=component.get("course_name"),
                academic_period=component.get("academic_period"),
            )

    for section in sections:
        if not section.get("teacher_id"):
            add_issue(issues, "teacher_missing", "У section не назначен преподаватель.", "error", section_id=section["id"])
        count = int(section.get("classes_count") or 0)
        if count < 1 or count > 4:
            add_issue(issues, "classes_count_invalid", "classes_count вне диапазона 1..4.", "error", section_id=section["id"], classes_count=count)

    for section in sections:
        lesson_type = normalize_lesson_type(section.get("lesson_type"))
        student_count = int(section.get("student_count") or 0)
        candidates = []
        for room in rooms:
            room_type = str(room.get("type") or "").strip().lower()
            if lesson_type == "lecture" and room_type != "lecture":
                continue
            if lesson_type == "practical" and room_type not in {"practical", "lecture"}:
                continue
            if lesson_type == "lab" and room_type != "practical":
                continue
            if lesson_type == "lab" and int(room.get("computer_count") or 0) < 10:
                continue
            if student_count and int(room.get("capacity") or 0) < student_count:
                continue
            candidates.append(room)
        if not candidates:
            code = "lab_overflow" if lesson_type == "lab" else "room_missing"
            add_issue(issues, code, "Нет подходящей аудитории для section.", "warning", section_id=section["id"], lesson_type=lesson_type)

    return {
        "summary": {
            "errors": sum(1 for issue in issues if issue["severity"] == "error"),
            "warnings": sum(1 for issue in issues if issue["severity"] == "warning"),
            "info": sum(1 for issue in issues if issue["severity"] == "info"),
            "sections": len(sections),
        },
        "issues": issues,
    }
