"""bootstrap core application schema

Revision ID: 20260504_0003
Revises: 20260504_0002
Create Date: 2026-05-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

from app.core.orm import Base
import app.models  # noqa: F401 - registers ORM models
from app.teachers.utils import build_teacher_name_signature, normalize_teacher_name


revision = "20260504_0003"
down_revision = "20260504_0002"
branch_labels = None
depends_on = None


def _inspector():
    return inspect(op.get_bind())


def _has_table(table_name):
    return table_name in _inspector().get_table_names()


def _columns(table_name):
    if not _has_table(table_name):
        return set()
    return {column["name"] for column in _inspector().get_columns(table_name)}


def _rename_column(table_name, old_name, new_name):
    columns = _columns(table_name)
    if old_name not in columns or new_name in columns:
        return
    op.execute(sa.text(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"))


def _add_column(table_name, column):
    if not _has_table(table_name) or column.name in _columns(table_name):
        return
    op.add_column(table_name, column)


def _create_index(index_name, table_name, columns):
    if not _has_table(table_name):
        return
    existing_indexes = {index["name"] for index in _inspector().get_indexes(table_name)}
    if index_name not in existing_indexes:
        op.create_index(index_name, table_name, columns)


def _scalar(statement, params=None):
    return op.get_bind().execute(sa.text(statement), params or {}).scalar()


def _execute(statement, params=None):
    op.get_bind().execute(sa.text(statement), params or {})


def _migrate_default_user_emails():
    for role, old_email, new_email in (
        ("admin", "admin@university.kz", "admin@kazatu.edu.kz"),
        ("teacher", "teacher@university.kz", "teacher@kazatu.edu.kz"),
    ):
        if _scalar("SELECT id FROM users WHERE lower(email) = lower(:email)", {"email": new_email}):
            continue
        _execute(
            """
            UPDATE users
            SET email = :new_email
            WHERE lower(email) = lower(:old_email) AND role = :role
            """,
            {"new_email": new_email, "old_email": old_email, "role": role},
        )


def _migrate_imported_teacher_emails():
    rows = op.get_bind().execute(
        sa.text(
            """
            SELECT id, email
            FROM teachers
            WHERE lower(email) LIKE :pattern
            ORDER BY id
            """
        ),
        {"pattern": "%@imported.local"},
    ).mappings()
    for teacher in rows:
        email = str(teacher.get("email") or "").strip().lower()
        local_part, _, _domain = email.partition("@")
        if not local_part:
            continue
        next_email = f"{local_part}@kazatu.edu.kz"
        existing = _scalar(
            "SELECT id FROM teachers WHERE lower(email) = lower(:email) AND id <> :id",
            {"email": next_email, "id": teacher["id"]},
        )
        if existing:
            continue
        _execute(
            "UPDATE teachers SET email = :email WHERE id = :id",
            {"email": next_email, "id": teacher["id"]},
        )


def _migrate_legacy_role_accounts():
    accounts = op.get_bind().execute(
        sa.text(
            """
            SELECT
                id, email, password, full_name, role, token, avatar_data,
                department, programme, group_id, group_name, subgroup
            FROM users
            WHERE role IN ('teacher', 'student')
            ORDER BY id
            """
        )
    ).mappings().all()

    for account in accounts:
        if account["role"] == "teacher":
            existing_teacher = _scalar(
                "SELECT id FROM teachers WHERE lower(email) = lower(:email)",
                {"email": account["email"]},
            )
            values = {
                "name": account["full_name"],
                "email": account["email"],
                "password": account["password"],
                "token": account["token"],
                "avatar_data": account.get("avatar_data"),
                "subject_taught": account.get("department") or "",
                "name_normalized": normalize_teacher_name(account["full_name"]),
                "name_signature": build_teacher_name_signature(account["full_name"]),
            }
            if existing_teacher is None:
                _execute(
                    """
                    INSERT INTO teachers (
                        name, email, password, token, avatar_data, phone,
                        subject_taught, weekly_hours_limit, name_normalized, name_signature
                    )
                    VALUES (
                        :name, :email, :password, :token, :avatar_data, '',
                        :subject_taught, NULL, :name_normalized, :name_signature
                    )
                    """,
                    values,
                )
            else:
                _execute(
                    """
                    UPDATE teachers
                    SET
                        name = COALESCE(NULLIF(name, ''), :name),
                        password = COALESCE(password, :password),
                        token = COALESCE(token, :token),
                        avatar_data = COALESCE(avatar_data, :avatar_data),
                        subject_taught = COALESCE(NULLIF(subject_taught, ''), :subject_taught),
                        name_normalized = COALESCE(NULLIF(name_normalized, ''), :name_normalized),
                        name_signature = COALESCE(NULLIF(name_signature, ''), :name_signature)
                    WHERE id = :id
                    """,
                    {**values, "id": existing_teacher},
                )
        else:
            existing_student = _scalar(
                "SELECT id FROM students WHERE lower(email) = lower(:email)",
                {"email": account["email"]},
            )
            values = {
                "name": account["full_name"],
                "email": account["email"],
                "password": account["password"],
                "token": account["token"],
                "avatar_data": account.get("avatar_data"),
                "department": account.get("department") or "",
                "programme": account.get("programme") or "",
                "group_id": account.get("group_id"),
                "group_name": account.get("group_name") or "",
                "subgroup": account.get("subgroup") or "",
            }
            if existing_student is None:
                _execute(
                    """
                    INSERT INTO students (
                        name, email, password, token, avatar_data, department,
                        programme, group_id, group_name, subgroup
                    )
                    VALUES (
                        :name, :email, :password, :token, :avatar_data, :department,
                        :programme, :group_id, :group_name, :subgroup
                    )
                    """,
                    values,
                )
            else:
                _execute(
                    """
                    UPDATE students
                    SET
                        name = COALESCE(NULLIF(name, ''), :name),
                        password = COALESCE(password, :password),
                        token = COALESCE(token, :token),
                        avatar_data = COALESCE(avatar_data, :avatar_data),
                        department = COALESCE(NULLIF(department, ''), :department),
                        programme = COALESCE(NULLIF(programme, ''), :programme),
                        group_id = COALESCE(group_id, :group_id),
                        group_name = COALESCE(NULLIF(group_name, ''), :group_name),
                        subgroup = COALESCE(NULLIF(subgroup, ''), :subgroup)
                    WHERE id = :id
                    """,
                    {**values, "id": existing_student},
                )

    if accounts:
        _execute("DELETE FROM users WHERE role IN ('teacher', 'student')")


def _ensure_columns():
    for table_name, old_name, new_name in (
        ("users", "display_name", "full_name"),
        ("users", "programme_name", "programme"),
        ("courses", "study_year", "year"),
        ("courses", "programme_name", "programme"),
        ("teachers", "specialization", "subject_taught"),
        ("teachers", "department", "subject_taught"),
        ("teachers", "max_hours_per_week", "weekly_hours_limit"),
        ("rooms", "is_available", "available"),
        ("rooms", "department", "programme"),
        ("sections", "class_count", "classes_count"),
    ):
        _rename_column(table_name, old_name, new_name)

    text = sa.Text()
    integer = sa.Integer()
    column_specs = {
        "users": [
            ("avatar_data", text),
            ("department", text),
            ("programme", text),
            ("group_id", integer),
            ("group_name", text),
            ("subgroup", text),
        ],
        "courses": [
            ("year", integer),
            ("semester", integer),
            ("department", text),
            ("instructor_id", integer),
            ("instructor_name", text),
            ("programme", text),
            ("module_type", text),
            ("module_name", text),
            ("cycle", text),
            ("component", text),
            ("language", text),
            ("academic_year", text),
            ("entry_year", text),
            ("requires_computers", integer, "0"),
        ],
        "teachers": [
            ("subject_taught", text),
            ("weekly_hours_limit", integer),
            ("name_normalized", text),
            ("name_signature", text),
            ("password", text),
            ("token", text),
            ("claim_code", text),
            ("claim_code_expires_at", text),
            ("claim_requested_at", text),
            ("avatar_data", text),
            ("teaching_languages", text, "'ru,kk'"),
        ],
        "students": [
            ("avatar_data", text),
            ("department", text),
            ("programme", text),
            ("group_id", integer),
            ("group_name", text),
            ("subgroup", text),
            ("language", text, "'ru'"),
        ],
        "rooms": [
            ("equipment", text),
            ("programme", text),
            ("available", integer, "1"),
            ("computer_count", integer, "0"),
        ],
        "groups": [
            ("has_subgroups", integer, "0"),
            ("language", text, "'ru'"),
            ("programme", text),
            ("specialty_code", text),
            ("entry_year", integer),
            ("study_course", integer),
        ],
        "sections": [
            ("group_id", integer),
            ("group_name", text),
            ("lesson_type", text, "'lecture'"),
            ("subgroup_mode", text, "'auto'"),
            ("subgroup_count", integer, "1"),
            ("requires_computers", integer, "0"),
            ("teacher_id", integer),
            ("teacher_name", text),
            ("iup_entry_id", integer),
            ("source", text, "'manual'"),
            ("match_method", text),
        ],
        "course_components": [
            ("teacher_id", integer),
            ("teacher_name", text),
        ],
        "iup_entries": [
            ("file_name", text),
            ("group_name", text),
            ("programme", text),
            ("study_course", integer),
            ("language", text),
            ("academic_year", text),
            ("academic_period", integer),
            ("semester", integer),
            ("component", text),
            ("course_code", text),
            ("course_name", text),
            ("credits", integer),
            ("lesson_type", text),
            ("teacher_id", integer),
            ("teacher_name", text),
            ("hours", integer),
        ],
        "schedules": [
            ("section_id", integer),
            ("group_id", integer),
            ("group_name", text),
            ("subgroup", text),
            ("room_programme", text),
            ("room_programme_mismatch", integer, "0"),
            ("relocated_from_room_number", text),
            ("relocation_reason", text),
        ],
        "room_blocks": [
            ("room_id", integer),
            ("day", text),
            ("start_hour", integer),
            ("end_hour", integer),
            ("semester", integer),
            ("year", integer),
            ("reason", text),
        ],
        "teacher_preference_requests": [
            ("note", text),
            ("status", text, "'pending'"),
            ("admin_comment", text),
            ("created_at", text),
            ("updated_at", text),
        ],
        "notifications": [
            ("recipient_role", text),
            ("recipient_id", integer),
            ("title", text),
            ("message", text),
            ("metadata", text),
            ("notification_type", text),
            ("is_read", integer, "0"),
            ("created_at", text),
            ("read_at", text),
        ],
        "schedule_generation_jobs": [
            ("status", text),
            ("semester", integer),
            ("year", integer),
            ("algorithm", text),
            ("created_at", text),
            ("updated_at", text),
            ("result", text),
            ("error", text),
            ("error_code", text),
            ("details", text),
            ("worker_id", text),
            ("started_at", text),
            ("finished_at", text),
        ],
    }
    for table_name, specs in column_specs.items():
        for spec in specs:
            name, column_type, *default = spec
            server_default = sa.text(default[0]) if default else None
            _add_column(
                table_name,
                sa.Column(name, column_type, nullable=True, server_default=server_default),
            )


def _data_cleanup():
    if _has_table("teachers"):
        rows = op.get_bind().execute(sa.text("SELECT id, name FROM teachers")).mappings().all()
        for teacher in rows:
            _execute(
                """
                UPDATE teachers
                SET name_normalized = :normalized, name_signature = :signature
                WHERE id = :id
                """,
                {
                    "normalized": normalize_teacher_name(teacher.get("name", "")),
                    "signature": build_teacher_name_signature(teacher.get("name", "")),
                    "id": teacher["id"],
                },
            )

    statements = [
        """
        UPDATE rooms
        SET type = 'practical'
        WHERE lower(COALESCE(type, '')) = 'lab'
        """,
        """
        UPDATE sections
        SET subgroup_mode = 'none', subgroup_count = 1
        WHERE lower(coalesce(lesson_type, 'lecture')) = 'lecture'
        """,
        """
        UPDATE sections
        SET subgroup_mode = 'auto'
        WHERE subgroup_mode IS NULL OR subgroup_mode = ''
        """,
        """
        UPDATE sections
        SET requires_computers = COALESCE(
            (
                SELECT cc.requires_computers
                FROM course_components cc
                WHERE cc.course_id = sections.course_id
                  AND cc.lesson_type = sections.lesson_type
                ORDER BY cc.academic_period, cc.id
                LIMIT 1
            ),
            CASE WHEN lesson_type = 'lab' THEN 1 ELSE 0 END
        )
        """,
        """
        UPDATE sections
        SET
            teacher_id = COALESCE(
                (
                    SELECT cc.teacher_id
                    FROM course_components cc
                    WHERE cc.course_id = sections.course_id
                      AND cc.lesson_type = sections.lesson_type
                      AND cc.teacher_id IS NOT NULL
                    ORDER BY cc.academic_period, cc.id
                    LIMIT 1
                ),
                (
                    SELECT c.instructor_id
                    FROM courses c
                    WHERE c.id = sections.course_id
                )
            ),
            teacher_name = COALESCE(
                (
                    SELECT cc.teacher_name
                    FROM course_components cc
                    WHERE cc.course_id = sections.course_id
                      AND cc.lesson_type = sections.lesson_type
                      AND cc.teacher_name IS NOT NULL
                      AND cc.teacher_name != ''
                    ORDER BY cc.academic_period, cc.id
                    LIMIT 1
                ),
                (
                    SELECT c.instructor_name
                    FROM courses c
                    WHERE c.id = sections.course_id
                ),
                ''
            )
        WHERE teacher_id IS NULL OR teacher_name IS NULL OR teacher_name = ''
        """,
        """
        UPDATE room_blocks
        SET end_hour = start_hour + 1
        WHERE end_hour IS NULL OR end_hour <= start_hour
        """,
        """
        DELETE FROM schedules
        WHERE section_id IN (
            SELECT id
            FROM sections
            WHERE lower(coalesce(lesson_type, '')) IN ('sro', 'srop', 'practice')
        )
        """,
        "DELETE FROM sections WHERE lower(coalesce(lesson_type, '')) IN ('sro', 'srop', 'practice')",
        "DELETE FROM course_components WHERE lower(coalesce(lesson_type, '')) = 'sro'",
        "DELETE FROM iup_entries WHERE lower(coalesce(lesson_type, '')) = 'sro'",
    ]
    for statement in statements:
        try:
            _execute(statement)
        except Exception:
            # Legacy cleanup is best-effort for partially migrated databases.
            pass

    if _has_table("users"):
        _migrate_default_user_emails()
        if _has_table("teachers"):
            _migrate_imported_teacher_emails()
        if _has_table("teachers") and _has_table("students"):
            _migrate_legacy_role_accounts()


def upgrade():
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind, checkfirst=True)
    _ensure_columns()
    _data_cleanup()

    for index_name, table_name, columns in (
        ("idx_courses_code", "courses", ["code"]),
        ("idx_course_components_code", "course_components", ["course_code"]),
        ("idx_iup_entries_code", "iup_entries", ["course_code"]),
        ("idx_sections_group_id", "sections", ["group_id"]),
        ("idx_sections_teacher_id", "sections", ["teacher_id"]),
        ("idx_schedules_group_id", "schedules", ["group_id"]),
        ("idx_schedules_teacher_id", "schedules", ["teacher_id"]),
        ("idx_teachers_name_signature", "teachers", ["name_signature"]),
        ("idx_schedule_generation_jobs_updated_at", "schedule_generation_jobs", ["updated_at"]),
    ):
        _create_index(index_name, table_name, columns)


def downgrade():
    # This is a baseline/bootstrap migration. Dropping core application tables
    # would be destructive, so downgrade intentionally does nothing.
    pass
