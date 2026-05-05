from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.core.orm import Base
import app.models  # noqa: F401 - registers ORM models for Alembic metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url():
    configured_url = os.environ.get("DATABASE_URL", "").strip()
    if configured_url:
        if configured_url.startswith("postgresql://"):
            return configured_url.replace("postgresql://", "postgresql+psycopg://", 1)
        if configured_url.startswith("postgres://"):
            return configured_url.replace("postgres://", "postgresql+psycopg://", 1)
        return configured_url

    sqlite_file = os.environ.get("SQLITE_DB_FILE", "").strip()
    if sqlite_file:
        return f"sqlite:///{sqlite_file}"

    db_file = Path(__file__).resolve().parents[1] / "data" / "timetable.db"
    return f"sqlite:///{db_file}"


def run_migrations_offline():
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
