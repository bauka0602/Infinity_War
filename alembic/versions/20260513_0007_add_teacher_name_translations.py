"""add teacher name translations

Revision ID: 20260513_0007
Revises: 20260513_0006
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0007"
down_revision = "20260513_0006"
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
    _add_missing_columns("teachers", ("name_kk", "name_en"))


def downgrade():
    _drop_existing_columns("teachers", ("name_en", "name_kk"))
