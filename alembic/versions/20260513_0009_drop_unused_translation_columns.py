"""drop unused translation columns

Revision ID: 20260513_0009
Revises: 20260513_0008
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0009"
down_revision = "20260513_0008"
branch_labels = None
depends_on = None


UNUSED_COLUMNS = {
    "teachers": [
        "name_kk",
        "name_en",
        "department_kk",
        "department_en",
        "subject_taught_kk",
        "subject_taught_en",
    ],
    "courses": [
        "name_kk",
        "name_en",
        "description_kk",
        "description_en",
        "programme_kk",
        "programme_en",
        "department_kk",
        "department_en",
        "module_name_kk",
        "module_name_en",
        "cycle_kk",
        "cycle_en",
        "component_kk",
        "component_en",
    ],
    "course_components": [
        "course_name_kk",
        "course_name_en",
        "programme_kk",
        "programme_en",
        "teacher_name_kk",
        "teacher_name_en",
    ],
    "iup_entries": [
        "student_name",
        "group_name_kk",
        "group_name_en",
        "programme_kk",
        "programme_en",
        "component_kk",
        "component_en",
        "course_name_kk",
        "course_name_en",
        "teacher_name_kk",
        "teacher_name_en",
    ],
    "rooms": [
        "number_kk",
        "number_en",
        "type_kk",
        "type_en",
        "equipment_kk",
        "equipment_en",
        "programme_kk",
        "programme_en",
    ],
    "groups": [
        "name_kk",
        "name_en",
        "programme_kk",
        "programme_en",
    ],
    "sections": [
        "course_name_kk",
        "course_name_en",
        "group_name_kk",
        "group_name_en",
        "teacher_name_kk",
        "teacher_name_en",
    ],
    "schedules": [
        "course_name_kk",
        "course_name_en",
        "teacher_name_kk",
        "teacher_name_en",
        "room_number_kk",
        "room_number_en",
        "group_name_kk",
        "group_name_en",
    ],
}


def _existing_tables_and_columns():
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    return {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in tables
    }


def upgrade():
    existing = _existing_tables_and_columns()
    for table_name, column_names in UNUSED_COLUMNS.items():
        table_columns = existing.get(table_name)
        if not table_columns:
            continue
        columns_to_drop = [name for name in column_names if name in table_columns]
        if not columns_to_drop:
            continue
        with op.batch_alter_table(table_name) as batch_op:
            for column_name in columns_to_drop:
                batch_op.drop_column(column_name)


def downgrade():
    existing = _existing_tables_and_columns()
    for table_name, column_names in UNUSED_COLUMNS.items():
        table_columns = existing.get(table_name)
        if table_columns is None:
            continue
        columns_to_add = [name for name in column_names if name not in table_columns]
        if not columns_to_add:
            continue
        with op.batch_alter_table(table_name) as batch_op:
            for column_name in columns_to_add:
                batch_op.add_column(sa.Column(column_name, sa.Text(), nullable=True))
