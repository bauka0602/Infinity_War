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
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("teachers")}
    if "department" in columns:
        return
    with op.batch_alter_table("teachers") as batch_op:
        batch_op.add_column(sa.Column("department", sa.Text(), nullable=True))


def downgrade():
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("teachers")}
    if "department" not in columns:
        return
    with op.batch_alter_table("teachers") as batch_op:
        batch_op.drop_column("department")
