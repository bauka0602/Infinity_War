"""create schedule generation jobs

Revision ID: 20260504_0001
Revises:
Create Date: 2026-05-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260504_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    inspector = inspect(op.get_bind())
    if "schedule_generation_jobs" in inspector.get_table_names():
        return

    op.create_table(
        "schedule_generation_jobs",
        sa.Column("job_id", sa.Text(), primary_key=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("semester", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("algorithm", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column("started_at", sa.Text(), nullable=True),
        sa.Column("finished_at", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_schedule_generation_jobs_updated_at",
        "schedule_generation_jobs",
        ["updated_at"],
    )


def downgrade():
    inspector = inspect(op.get_bind())
    if "schedule_generation_jobs" not in inspector.get_table_names():
        return

    existing_indexes = {
        index["name"]
        for index in inspector.get_indexes("schedule_generation_jobs")
    }
    if "idx_schedule_generation_jobs_updated_at" in existing_indexes:
        op.drop_index(
            "idx_schedule_generation_jobs_updated_at",
            table_name="schedule_generation_jobs",
        )
    op.drop_table("schedule_generation_jobs")
