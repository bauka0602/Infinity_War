from __future__ import annotations

from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from ..core.orm import Base


class ScheduleGenerationJob(Base):
    __tablename__ = "schedule_generation_jobs"

    job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    semester: Mapped[int] = mapped_column(Integer, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    algorithm: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    finished_at: Mapped[str | None] = mapped_column(Text, nullable=True)
