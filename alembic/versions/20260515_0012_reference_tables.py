"""add editable reference tables

Revision ID: 20260515_0012
Revises: 20260514_0011
Create Date: 2026-05-15
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "20260515_0012"
down_revision = "20260514_0011"
branch_labels = None
depends_on = None


REFERENCE_TABLES = [
    "reference_room_programmes",
    "reference_educational_programmes",
    "reference_education_groups",
    "reference_study_languages",
    "reference_faculties",
]


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _defaults() -> dict[str, object]:
    path = Path(__file__).resolve().parents[2] / "app" / "reference" / "defaults.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _legacy_payload() -> dict[str, object]:
    bind = op.get_bind()
    if not _has_table("reference_datasets"):
        return {}

    rows = bind.execute(text("select key, data_json from reference_datasets")).mappings().all()
    payload: dict[str, object] = {}
    for row in rows:
        try:
            payload[row["key"]] = json.loads(row["data_json"])
        except (TypeError, json.JSONDecodeError):
            continue
    return payload


def _reference_payload() -> dict[str, object]:
    payload = _legacy_payload()
    defaults = _defaults()
    return {**defaults, **payload}


def _table_empty(table_name: str) -> bool:
    bind = op.get_bind()
    count = bind.execute(text(f"select count(*) from {table_name}")).scalar_one()
    return int(count or 0) == 0


def _create_reference_tables() -> None:
    if not _has_table("reference_faculties"):
        op.create_table(
            "reference_faculties",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("value", sa.Text(), nullable=False, unique=True),
            sa.Column("label_ru", sa.Text(), nullable=False),
            sa.Column("label_kk", sa.Text(), nullable=True),
            sa.Column("label_en", sa.Text(), nullable=True),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )

    if not _has_table("reference_study_languages"):
        op.create_table(
            "reference_study_languages",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("value", sa.Text(), nullable=False, unique=True),
            sa.Column("label_key", sa.Text(), nullable=False),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )

    if not _has_table("reference_education_groups"):
        op.create_table(
            "reference_education_groups",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("code", sa.Text(), nullable=False, unique=True),
            sa.Column("label_ru", sa.Text(), nullable=False),
            sa.Column("label_kk", sa.Text(), nullable=True),
            sa.Column("label_en", sa.Text(), nullable=True),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )

    if not _has_table("reference_educational_programmes"):
        op.create_table(
            "reference_educational_programmes",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("code", sa.Text(), nullable=False, unique=True),
            sa.Column("education_group_code", sa.Text(), nullable=False),
            sa.Column("label_ru", sa.Text(), nullable=False),
            sa.Column("label_kk", sa.Text(), nullable=True),
            sa.Column("label_en", sa.Text(), nullable=True),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )
        op.create_index(
            "ix_reference_educational_programmes_group",
            "reference_educational_programmes",
            ["education_group_code"],
        )

    if not _has_table("reference_room_programmes"):
        op.create_table(
            "reference_room_programmes",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("value", sa.Text(), nullable=False, unique=True),
            sa.Column("label_ru", sa.Text(), nullable=False),
            sa.Column("label_kk", sa.Text(), nullable=True),
            sa.Column("label_en", sa.Text(), nullable=True),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )


def _seed_reference_tables() -> None:
    payload = _reference_payload()
    now = _utc_now()
    bind = op.get_bind()

    if _table_empty("reference_faculties"):
        rows = []
        for index, faculty in enumerate(payload.get("faculties") or []):
            labels = faculty.get("labels") or {}
            rows.append(
                {
                    "value": faculty.get("value") or labels.get("ru"),
                    "label_ru": labels.get("ru") or faculty.get("value"),
                    "label_kk": labels.get("kk"),
                    "label_en": labels.get("en"),
                    "sort_order": index,
                    "is_active": 1,
                    "updated_at": now,
                }
            )
        if rows:
            bind.execute(sa.table(
                "reference_faculties",
                sa.column("value"),
                sa.column("label_ru"),
                sa.column("label_kk"),
                sa.column("label_en"),
                sa.column("sort_order"),
                sa.column("is_active"),
                sa.column("updated_at"),
            ).insert(), rows)

    if _table_empty("reference_study_languages"):
        rows = [
            {
                "value": item.get("value"),
                "label_key": item.get("labelKey"),
                "sort_order": index,
                "is_active": 1,
                "updated_at": now,
            }
            for index, item in enumerate(payload.get("studyLanguages") or [])
            if item.get("value") and item.get("labelKey")
        ]
        if rows:
            bind.execute(sa.table(
                "reference_study_languages",
                sa.column("value"),
                sa.column("label_key"),
                sa.column("sort_order"),
                sa.column("is_active"),
                sa.column("updated_at"),
            ).insert(), rows)

    education_group_labels = payload.get("educationGroupLabels") or {}
    if _table_empty("reference_education_groups"):
        rows = []
        for index, group in enumerate(payload.get("educationGroups") or []):
            code = group.get("value")
            labels = education_group_labels.get(code) or {}
            rows.append(
                {
                    "code": code,
                    "label_ru": group.get("label"),
                    "label_kk": labels.get("kk"),
                    "label_en": labels.get("en"),
                    "sort_order": index,
                    "is_active": 1,
                    "updated_at": now,
                }
            )
        if rows:
            bind.execute(sa.table(
                "reference_education_groups",
                sa.column("code"),
                sa.column("label_ru"),
                sa.column("label_kk"),
                sa.column("label_en"),
                sa.column("sort_order"),
                sa.column("is_active"),
                sa.column("updated_at"),
            ).insert(), rows)

    programme_labels = payload.get("programmeLabels") or {}
    if _table_empty("reference_educational_programmes"):
        rows = []
        for group in payload.get("educationGroups") or []:
            group_code = group.get("value")
            for index, programme in enumerate(group.get("programmes") or []):
                code = programme.get("code")
                labels = programme_labels.get(code) or {}
                rows.append(
                    {
                        "code": code,
                        "education_group_code": group_code,
                        "label_ru": programme.get("label"),
                        "label_kk": labels.get("kk"),
                        "label_en": labels.get("en"),
                        "sort_order": index,
                        "is_active": 1,
                        "updated_at": now,
                    }
                )
        if rows:
            bind.execute(sa.table(
                "reference_educational_programmes",
                sa.column("code"),
                sa.column("education_group_code"),
                sa.column("label_ru"),
                sa.column("label_kk"),
                sa.column("label_en"),
                sa.column("sort_order"),
                sa.column("is_active"),
                sa.column("updated_at"),
            ).insert(), rows)

    if _table_empty("reference_room_programmes"):
        rows = []
        for index, programme in enumerate(payload.get("roomProgrammes") or []):
            labels = programme.get("labels") or {}
            rows.append(
                {
                    "value": programme.get("value"),
                    "label_ru": labels.get("ru") or programme.get("value"),
                    "label_kk": labels.get("kk"),
                    "label_en": labels.get("en"),
                    "sort_order": index,
                    "is_active": 1,
                    "updated_at": now,
                }
            )
        if rows:
            bind.execute(sa.table(
                "reference_room_programmes",
                sa.column("value"),
                sa.column("label_ru"),
                sa.column("label_kk"),
                sa.column("label_en"),
                sa.column("sort_order"),
                sa.column("is_active"),
                sa.column("updated_at"),
            ).insert(), rows)


def upgrade():
    _create_reference_tables()
    _seed_reference_tables()


def downgrade():
    for table_name in REFERENCE_TABLES:
        if _has_table(table_name):
            op.drop_table(table_name)
