"""drop users legacy profile columns

Revision ID: 20260513_0010
Revises: 20260513_0009
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0010"
down_revision = "20260513_0009"
branch_labels = None
depends_on = None


LEGACY_USER_COLUMNS = [
    "department",
    "programme",
    "group_id",
    "group_name",
    "subgroup",
]


def _user_columns():
    inspector = inspect(op.get_bind())
    if "users" not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns("users")}


def upgrade():
    existing_columns = _user_columns()
    columns_to_drop = [name for name in LEGACY_USER_COLUMNS if name in existing_columns]
    if not columns_to_drop:
        return
    with op.batch_alter_table("users") as batch_op:
        for column_name in columns_to_drop:
            batch_op.drop_column(column_name)


def downgrade():
    existing_columns = _user_columns()
    columns_to_add = [name for name in LEGACY_USER_COLUMNS if name not in existing_columns]
    if not columns_to_add:
        return
    with op.batch_alter_table("users") as batch_op:
        for column_name in columns_to_add:
            column_type = sa.Integer() if column_name == "group_id" else sa.Text()
            batch_op.add_column(sa.Column(column_name, column_type, nullable=True))
