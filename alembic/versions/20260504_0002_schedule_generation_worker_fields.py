"""add schedule generation worker fields

Revision ID: 20260504_0002
Revises: 20260504_0001
Create Date: 2026-05-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260504_0002"
down_revision = "20260504_0001"
branch_labels = None
depends_on = None


def _existing_columns():
    inspector = inspect(op.get_bind())
    if "schedule_generation_jobs" not in inspector.get_table_names():
        return set()
    return {
        column["name"]
        for column in inspector.get_columns("schedule_generation_jobs")
    }


def upgrade():
    existing_columns = _existing_columns()
    if not existing_columns:
        return

    for column_name in ("worker_id", "started_at", "finished_at"):
        if column_name not in existing_columns:
            op.add_column(
                "schedule_generation_jobs",
                sa.Column(column_name, sa.Text(), nullable=True),
            )


def downgrade():
    existing_columns = _existing_columns()
    for column_name in ("finished_at", "started_at", "worker_id"):
        if column_name in existing_columns:
            op.drop_column("schedule_generation_jobs", column_name)
