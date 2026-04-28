import hashlib
import logging
import re
from difflib import SequenceMatcher

from .collections import normalize_lesson_type, normalize_language, positive_int
from .db import db_execute, insert_and_get_id, query_all, query_one
from .errors import ApiError
from .lesson_rules import requires_computers_for_component
from .programme_utils import normalize_programme_text, same_programme
from .teacher_utils import build_teacher_name_signature, normalize_teacher_name

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


def _load_rop_components(connection, payload):
    clauses = ["cc.lesson_type IN ('lecture', 'practical', 'lab')"]
    params = []
    if payload.get("academic_period"):
        clauses.append("cc.academic_period = ?")
        params.append(int(payload["academic_period"]))
    if payload.get("semester"):
        clauses.append("cc.semester = ?")
        params.append(int(payload["semester"]))
    if payload.get("programme"):
        clauses.append("coalesce(cc.programme, c.programme, '') <> ''")

    return query_all(
        connection,
        f"""
        SELECT
            cc.id AS component_id,
            cc.course_id,
            cc.course_code,
            cc.course_name,
            cc.programme,
            cc.study_year,
            cc.academic_period,
            cc.semester,
            cc.lesson_type,
            cc.hours,
            cc.weekly_classes,
            cc.requires_computers,
            c.language,
            c.component,
            c.programme AS course_programme
        FROM course_components cc
        JOIN courses c ON c.id = cc.course_id
        WHERE {" AND ".join(clauses)}
        ORDER BY cc.academic_period, cc.course_name, cc.lesson_type, cc.id
        """,
        tuple(params),
    )


def _load_iup_entries(connection, payload):
    clauses = ["lesson_type IN ('lecture', 'practical', 'lab')"]
    params = []
    for field in ("group_name", "academic_period", "semester", "programme", "language"):
        value = payload.get(field)
        if value in (None, ""):
            continue
        if field in {"academic_period", "semester"}:
            clauses.append(f"{field} = ?")
            params.append(int(value))
        else:
            clauses.append(f"lower(coalesce({field}, '')) = lower(?)")
            params.append(str(value).strip())
    return query_all(
        connection,
        f"""
        SELECT
            id,
            file_name,
            group_name,
            programme,
            study_course,
            language,
            academic_year,
            academic_period,
            semester,
            component,
            course_code,
            course_name,
            credits,
            lesson_type,
            teacher_id,
            teacher_name,
            hours
        FROM iup_entries
        WHERE {" AND ".join(clauses)}
        ORDER BY group_name, academic_period, course_name, lesson_type, id
        """,
        tuple(params),
    )


def _build_component_index(components):
    index = {}
    by_lesson_period = {}
    for component in components:
        key = (
            normalize_course_code(component.get("course_code")),
            int(component.get("academic_period") or 0),
            normalize_lesson_type(component.get("lesson_type")),
            normalize_language(component.get("language"), ""),
            normalize_programme_text(component.get("programme") or component.get("course_programme")),
        )
        index.setdefault(key, []).append(component)
        by_lesson_period.setdefault((key[1], key[2], key[3], key[4]), []).append(component)
    return index, by_lesson_period


def _match_component(iup_entry, component_index, by_lesson_period, issues, strict_mode):
    key = (
        normalize_course_code(iup_entry.get("course_code")),
        int(iup_entry.get("academic_period") or 0),
        normalize_lesson_type(iup_entry.get("lesson_type")),
        normalize_language(iup_entry.get("language"), ""),
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


def _find_group(connection, iup_entry, issues):
    group_name = str(iup_entry.get("group_name") or "").strip()
    if not group_name:
        add_issue(issues, "group_missing", "В записи ИУП не указана группа.", "error", iup_entry_id=iup_entry.get("id"))
        return None
    group = query_one(
        connection,
        """
        SELECT id, name, student_count, language, programme, specialty_code, study_course
        FROM groups
        WHERE lower(name) = lower(?)
        """,
        (group_name,),
    )
    if not group:
        add_issue(issues, "group_missing", "Группа из ИУП не найдена в БД.", "error", group_name=group_name)
        return None
    if not _same_language(group.get("language"), iup_entry.get("language")):
        add_issue(issues, "language_mismatch", "Язык группы и дисциплины ИУП не совпадает.", "error", group_name=group_name)
        return None
    if not _same_programme(group.get("programme"), iup_entry.get("programme")):
        add_issue(issues, "programme_mismatch", "Программа группы и дисциплины ИУП не совпадает.", "error", group_name=group_name)
        return None
    return group


def _find_or_create_teacher(connection, iup_entry, issues):
    teacher_id = iup_entry.get("teacher_id")
    if teacher_id:
        teacher = query_one(connection, "SELECT id, name FROM teachers WHERE id = ?", (teacher_id,))
        if teacher:
            return teacher

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
    teacher = query_one(
        connection,
        "SELECT id, name FROM teachers WHERE name_signature = ? ORDER BY id LIMIT 1",
        (signature,),
    )
    if teacher:
        return teacher

    digest = hashlib.sha1(normalize_teacher_name(teacher_name).encode("utf-8")).hexdigest()[:10]
    email = f"teacher-{digest}@kazatu.edu.kz"
    existing_email = query_one(connection, "SELECT id, name FROM teachers WHERE lower(email) = lower(?)", (email,))
    if existing_email:
        return existing_email

    teacher_id = insert_and_get_id(
        connection,
        """
        INSERT INTO teachers (
            name, email, phone, subject_taught, weekly_hours_limit,
            name_normalized, name_signature, teaching_languages
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            teacher_name,
            email,
            "",
            iup_entry.get("course_name") or "",
            None,
            normalize_teacher_name(teacher_name),
            signature,
            normalize_language(iup_entry.get("language"), "ru"),
        ),
    )
    add_issue(issues, "teacher_created", "Преподаватель из ИУП создан в БД.", "info", teacher_id=teacher_id, teacher_name=teacher_name)
    return {"id": teacher_id, "name": teacher_name}


def _find_teacher_for_preview(connection, iup_entry, issues):
    teacher_id = iup_entry.get("teacher_id")
    if teacher_id:
        teacher = query_one(connection, "SELECT id, name FROM teachers WHERE id = ?", (teacher_id,))
        if teacher:
            return teacher

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
    teacher = query_one(
        connection,
        "SELECT id, name FROM teachers WHERE name_signature = ? ORDER BY id LIMIT 1",
        (signature,),
    )
    if teacher:
        return teacher

    add_issue(
        issues,
        "teacher_would_be_created",
        "Преподаватель из ИУП будет создан при генерации.",
        "info",
        iup_entry_id=iup_entry.get("id"),
        teacher_name=teacher_name,
    )
    return {"id": None, "name": teacher_name}


def _fallback_component_from_iup(connection, iup_entry, preview=False):
    course = query_one(
        connection,
        """
        SELECT id
        FROM courses
        WHERE lower(code) = lower(?)
          AND semester = ?
          AND lower(coalesce(programme, '')) = lower(?)
        ORDER BY id
        LIMIT 1
        """,
        (
            iup_entry.get("course_code") or iup_entry.get("course_name"),
            iup_entry.get("academic_period"),
            iup_entry.get("programme") or "",
        ),
    )
    if course:
        course_id = course["id"]
    elif preview:
        course_id = None
    else:
        course_id = insert_and_get_id(
            connection,
            """
            INSERT INTO courses (
                name, code, credits, hours, description, year, semester, department,
                instructor_id, instructor_name, programme, module_type, module_name,
                cycle, component, language, academic_year, entry_year, requires_computers
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                iup_entry.get("course_name"),
                iup_entry.get("course_code") or iup_entry.get("course_name"),
                iup_entry.get("credits"),
                iup_entry.get("hours"),
                "Fallback course created from IUP without ROP match.",
                iup_entry.get("study_course"),
                iup_entry.get("academic_period"),
                "",
                None,
                "",
                iup_entry.get("programme") or "",
                "",
                "",
                "",
                iup_entry.get("component") or "",
                iup_entry.get("language") or "",
                iup_entry.get("academic_year") or "",
                "",
                1 if requires_computers_for_component(
                    iup_entry.get("lesson_type"),
                    iup_entry.get("course_code"),
                    iup_entry.get("course_name"),
                    iup_entry.get("study_course"),
                ) else 0,
            ),
        )
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
    components = _load_rop_components(connection, payload)
    iup_entries = _load_iup_entries(connection, payload)
    component_index, by_lesson_period = _build_component_index(components)

    if not iup_entries:
        raise ApiError(400, "iup_entries_missing", "Нет записей ИУП для генерации sections.")
    if not components and strict_mode:
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
        if normalize_component(iup_entry.get("component")) in ELECTIVE_COMPONENTS:
            electives_by_group[iup_entry.get("group_name")] = electives_by_group.get(iup_entry.get("group_name"), 0) + 1

        group = _find_group(connection, iup_entry, issues)
        if not group:
            skipped += 1
            continue
        component, match_method = _match_component(iup_entry, component_index, by_lesson_period, issues, strict_mode)
        if component is None:
            if strict_mode:
                skipped += 1
                continue
            add_issue(issues, "fallback_iup_course_used", "Section будет создан по ИУП без найденного РОП.", "warning", iup_entry_id=iup_entry["id"])
            component = _fallback_component_from_iup(connection, iup_entry, preview=preview)
            match_method = "fallback_iup"

        teacher = _find_teacher_for_preview(connection, iup_entry, issues) if preview else _find_or_create_teacher(connection, iup_entry, issues)
        if not teacher:
            skipped += 1
            continue

        classes_count = _classes_count(iup_entry, component)
        if classes_count < 1 or classes_count > 4:
            add_issue(issues, "classes_count_invalid", "classes_count скорректирован до диапазона 1..4.", "warning", iup_entry_id=iup_entry["id"])
        requires_computers = 1 if (
            component.get("requires_computers")
            or requires_computers_for_component(lesson_type, component.get("course_code"), component.get("course_name"), component.get("study_year"))
        ) else 0
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

        existing = query_one(
            connection,
            """
            SELECT id
            FROM sections
            WHERE course_id = ? AND group_id = ? AND lesson_type = ?
            LIMIT 1
            """,
            (section["course_id"], section["group_id"], section["lesson_type"]),
        )
        if preview:
            action = "updated" if existing else "inserted"
            section_id = existing["id"] if existing else None
        else:
            section_id, action = _insert_or_update_section(connection, section)
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
            normalize_language(entry.get("language"), ""),
            normalize_programme_text(entry.get("programme")),
        )
        for entry in iup_entries
    }
    for component in components:
        component_key = (
            normalize_course_code(component.get("course_code")),
            int(component.get("academic_period") or 0),
            normalize_lesson_type(component.get("lesson_type")),
            normalize_language(component.get("language"), ""),
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
        connection.commit()

    return {
        "preview": preview,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "strict_mode": strict_mode,
        "issues": issues,
        "sections": generated_sections,
    }


def _insert_or_update_section(connection, section):
    existing = query_one(
        connection,
        """
        SELECT id
        FROM sections
        WHERE course_id = ? AND group_id = ? AND lesson_type = ?
        LIMIT 1
        """,
        (section["course_id"], section["group_id"], section["lesson_type"]),
    )
    values = (
        section["course_id"],
        section["course_name"],
        section["group_id"],
        section["group_name"],
        section["classes_count"],
        section["lesson_type"],
        section["subgroup_mode"],
        section["subgroup_count"],
        section["requires_computers"],
        section["teacher_id"],
        section["teacher_name"],
        section["iup_entry_id"],
        section["source"],
        section["match_method"],
    )
    if existing:
        db_execute(
            connection,
            """
            UPDATE sections
            SET course_id = ?, course_name = ?, group_id = ?, group_name = ?,
                classes_count = ?, lesson_type = ?, subgroup_mode = ?, subgroup_count = ?,
                requires_computers = ?, teacher_id = ?, teacher_name = ?,
                iup_entry_id = ?, source = ?, match_method = ?
            WHERE id = ?
            """,
            (*values, existing["id"]),
        )
        return existing["id"], "updated"

    section_id = insert_and_get_id(
        connection,
        """
        INSERT INTO sections (
            course_id, course_name, group_id, group_name, classes_count,
            lesson_type, subgroup_mode, subgroup_count, requires_computers,
            teacher_id, teacher_name, iup_entry_id, source, match_method
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    return section_id, "inserted"


def generate_sections_from_iup(connection, payload):
    return _build_sections_from_iup(connection, payload, preview=False)


def preview_sections_from_iup(connection, payload):
    return _build_sections_from_iup(connection, payload, preview=True)


def build_validation_report(connection):
    issues = []
    components = _load_rop_components(connection, {})
    iup_entries = _load_iup_entries(connection, {})
    component_index, _by_lesson_period = _build_component_index(components)
    iup_keys = set()
    for entry in iup_entries:
        key = (
            normalize_course_code(entry.get("course_code")),
            int(entry.get("academic_period") or 0),
            normalize_lesson_type(entry.get("lesson_type")),
            normalize_language(entry.get("language"), ""),
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
            normalize_language(component.get("language"), ""),
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

    sections = query_all(
        connection,
        """
        SELECT
            s.id, s.course_id, s.course_name, s.group_id, s.group_name,
            s.lesson_type, s.classes_count, s.teacher_id, s.teacher_name,
            s.requires_computers, g.student_count
        FROM sections s
        LEFT JOIN groups g ON g.id = s.group_id
        ORDER BY s.id
        """,
    )
    for section in sections:
        if not section.get("teacher_id"):
            add_issue(issues, "teacher_missing", "У section не назначен преподаватель.", "error", section_id=section["id"])
        count = int(section.get("classes_count") or 0)
        if count < 1 or count > 4:
            add_issue(issues, "classes_count_invalid", "classes_count вне диапазона 1..4.", "error", section_id=section["id"], classes_count=count)

    rooms = query_all(connection, "SELECT id, number, capacity, type, computer_count, available FROM rooms WHERE coalesce(available, 1) = 1")
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
