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
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("rooms")}
    if "building" in columns:
        return
    with op.batch_alter_table("rooms") as batch_op:
        batch_op.add_column(sa.Column("building", sa.Text(), nullable=True))


def downgrade():
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("rooms")}
    if "building" not in columns:
        return
    with op.batch_alter_table("rooms") as batch_op:
        batch_op.drop_column("building")
