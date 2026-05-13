"""add course meta translations

Revision ID: 20260513_0008
Revises: 20260513_0007
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0008"
down_revision = "20260513_0007"
branch_labels = None
depends_on = None


def _add_missing_columns(table_name, columns):
    existing = {column["name"] for column in inspect(op.get_bind()).get_columns(table_name)}
    with op.batch_alter_table(table_name) as batch_op:
        for name in columns:
            if name not in existing:
                batch_op.add_column(sa.Column(name, sa.Text(), nullable=True))


def _drop_existing_columns(table_name, columns):
    existing = {column["name"] for column in inspect(op.get_bind()).get_columns(table_name)}
    with op.batch_alter_table(table_name) as batch_op:
        for name in columns:
            if name in existing:
                batch_op.drop_column(name)


def upgrade():
    _add_missing_columns(
        "courses",
        (
            "programme_kk",
            "programme_en",
            "cycle_kk",
            "cycle_en",
            "component_kk",
            "component_en",
            "department_kk",
            "department_en",
        ),
    )


def downgrade():
    _drop_existing_columns(
        "courses",
        (
            "department_en",
            "department_kk",
            "component_en",
            "component_kk",
            "cycle_en",
            "cycle_kk",
            "programme_en",
            "programme_kk",
        ),
    )
