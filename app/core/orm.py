from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import DATABASE_URL, DB_FILE


def sqlalchemy_url():
    if DATABASE_URL.startswith("postgresql://"):
        return DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
    if DATABASE_URL.startswith("postgres://"):
        return DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
    return f"sqlite:///{Path(DB_FILE)}"


def engine_kwargs():
    if sqlalchemy_url().startswith("sqlite:///"):
        return {"connect_args": {"check_same_thread": False}}
    return {}


class Base(DeclarativeBase):
    pass


engine = create_engine(sqlalchemy_url(), future=True, **engine_kwargs())
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


def get_session():
    with SessionLocal() as session:
        yield session
