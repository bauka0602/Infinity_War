"""add teacher department

Revision ID: 20260513_0005
Revises: 20260513_0004
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0005"
down_revision = "20260513_0004"
branch_labels = None
depends_on = None


def upgrade():
    inspector = inspect(op.get_bind())
    if "teachers" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("teachers")}
    if "department" not in columns:
        op.add_column("teachers", sa.Column("department", sa.Text(), nullable=True))


def downgrade():
    inspector = inspect(op.get_bind())
    if "teachers" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("teachers")}
    if "department" in columns:
        op.drop_column("teachers", "department")
