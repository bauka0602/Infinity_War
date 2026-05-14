"""add reference datasets

Revision ID: 20260514_0011
Revises: 20260513_0010
Create Date: 2026-05-14
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260514_0011"
down_revision = "20260513_0010"
branch_labels = None
depends_on = None


def _has_table(name):
    return name in inspect(op.get_bind()).get_table_names()


def upgrade():
    if _has_table("reference_datasets"):
        return
    op.create_table(
        "reference_datasets",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("data_json", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
    )


def downgrade():
    if _has_table("reference_datasets"):
        op.drop_table("reference_datasets")
