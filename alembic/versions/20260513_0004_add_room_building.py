"""add room building

Revision ID: 20260513_0004
Revises: 20260504_0003
Create Date: 2026-05-13
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260513_0004"
down_revision = "20260504_0003"
branch_labels = None
depends_on = None


def upgrade():
    inspector = inspect(op.get_bind())
    if "rooms" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("rooms")}
    if "building" not in columns:
        op.add_column("rooms", sa.Column("building", sa.Text(), nullable=True))


def downgrade():
    inspector = inspect(op.get_bind())
    if "rooms" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("rooms")}
    if "building" in columns:
        op.drop_column("rooms", "building")
