from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..core.orm import SessionLocal
from ..models import (
    ReferenceDataset,
    ReferenceEducationGroup,
    ReferenceEducationalProgramme,
    ReferenceFaculty,
    ReferenceRoomProgramme,
    ReferenceStudyLanguage,
)


DEFAULTS_FILE = Path(__file__).with_name("defaults.json")


def _load_default_references() -> dict[str, object]:
    if not DEFAULTS_FILE.exists():
        return {}
    return json.loads(DEFAULTS_FILE.read_text(encoding="utf-8"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_reference_datasets() -> None:
    defaults = _load_default_references()
    with SessionLocal() as session:
        existing_keys = set(session.scalars(select(ReferenceDataset.key)).all())
        missing_items = [
            ReferenceDataset(
                key=key,
                data_json=json.dumps(value, ensure_ascii=False),
                updated_at=_utc_now(),
            )
            for key, value in defaults.items()
            if key not in existing_keys
        ]
        if missing_items:
            session.add_all(missing_items)
            session.commit()


def _labels(row) -> dict[str, str]:
    return {
        key: value
        for key, value in {
            "ru": row.label_ru,
            "kk": row.label_kk,
            "en": row.label_en,
        }.items()
        if value
    }


def _seed_reference_tables_from_payload(session: Session, payload: dict[str, object]) -> None:
    now = _utc_now()

    if not session.scalar(select(func.count()).select_from(ReferenceFaculty)):
        for index, faculty in enumerate(payload.get("faculties") or []):
            labels = faculty.get("labels") or {}
            value = faculty.get("value") or labels.get("ru")
            if not value:
                continue
            session.add(
                ReferenceFaculty(
                    value=value,
                    label_ru=labels.get("ru") or value,
                    label_kk=labels.get("kk"),
                    label_en=labels.get("en"),
                    sort_order=index,
                    is_active=1,
                    updated_at=now,
                )
            )

    if not session.scalar(select(func.count()).select_from(ReferenceStudyLanguage)):
        for index, item in enumerate(payload.get("studyLanguages") or []):
            if not item.get("value") or not item.get("labelKey"):
                continue
            session.add(
                ReferenceStudyLanguage(
                    value=item["value"],
                    label_key=item["labelKey"],
                    sort_order=index,
                    is_active=1,
                    updated_at=now,
                )
            )

    education_group_labels = payload.get("educationGroupLabels") or {}
    if not session.scalar(select(func.count()).select_from(ReferenceEducationGroup)):
        for index, group in enumerate(payload.get("educationGroups") or []):
            code = group.get("value")
            if not code or not group.get("label"):
                continue
            labels = education_group_labels.get(code) or {}
            session.add(
                ReferenceEducationGroup(
                    code=code,
                    label_ru=group["label"],
                    label_kk=labels.get("kk"),
                    label_en=labels.get("en"),
                    sort_order=index,
                    is_active=1,
                    updated_at=now,
                )
            )

    programme_labels = payload.get("programmeLabels") or {}
    if not session.scalar(select(func.count()).select_from(ReferenceEducationalProgramme)):
        for group in payload.get("educationGroups") or []:
            group_code = group.get("value")
            if not group_code:
                continue
            for index, programme in enumerate(group.get("programmes") or []):
                code = programme.get("code")
                if not code or not programme.get("label"):
                    continue
                labels = programme_labels.get(code) or {}
                session.add(
                    ReferenceEducationalProgramme(
                        code=code,
                        education_group_code=group_code,
                        label_ru=programme["label"],
                        label_kk=labels.get("kk"),
                        label_en=labels.get("en"),
                        sort_order=index,
                        is_active=1,
                        updated_at=now,
                    )
                )

    if not session.scalar(select(func.count()).select_from(ReferenceRoomProgramme)):
        for index, programme in enumerate(payload.get("roomProgrammes") or []):
            labels = programme.get("labels") or {}
            value = programme.get("value")
            if not value:
                continue
            session.add(
                ReferenceRoomProgramme(
                    value=value,
                    label_ru=labels.get("ru") or value,
                    label_kk=labels.get("kk"),
                    label_en=labels.get("en"),
                    sort_order=index,
                    is_active=1,
                    updated_at=now,
                )
            )


def _legacy_reference_payload(session: Session) -> dict[str, object]:
    rows = session.scalars(select(ReferenceDataset)).all()
    return {row.key: json.loads(row.data_json) for row in rows}


def ensure_reference_tables() -> None:
    defaults = _load_default_references()
    with SessionLocal() as session:
        payload = {**defaults, **_legacy_reference_payload(session)}
        _seed_reference_tables_from_payload(session, payload)
        session.commit()


def _reference_payload_from_tables(session: Session) -> dict[str, object]:
    faculties = [
        {"value": row.value, "labels": _labels(row)}
        for row in session.scalars(
            select(ReferenceFaculty)
            .where(ReferenceFaculty.is_active == 1)
            .order_by(ReferenceFaculty.sort_order, ReferenceFaculty.id)
        ).all()
    ]

    study_languages = [
        {"value": row.value, "labelKey": row.label_key}
        for row in session.scalars(
            select(ReferenceStudyLanguage)
            .where(ReferenceStudyLanguage.is_active == 1)
            .order_by(ReferenceStudyLanguage.sort_order, ReferenceStudyLanguage.id)
        ).all()
    ]

    groups = session.scalars(
        select(ReferenceEducationGroup)
        .where(ReferenceEducationGroup.is_active == 1)
        .order_by(ReferenceEducationGroup.sort_order, ReferenceEducationGroup.id)
    ).all()
    programmes = session.scalars(
        select(ReferenceEducationalProgramme)
        .where(ReferenceEducationalProgramme.is_active == 1)
        .order_by(
            ReferenceEducationalProgramme.education_group_code,
            ReferenceEducationalProgramme.sort_order,
            ReferenceEducationalProgramme.id,
        )
    ).all()
    programmes_by_group: dict[str, list[ReferenceEducationalProgramme]] = {}
    for programme in programmes:
        programmes_by_group.setdefault(programme.education_group_code, []).append(programme)

    education_groups = [
        {
            "value": group.code,
            "label": group.label_ru,
            "programmes": [
                {"code": programme.code, "label": programme.label_ru}
                for programme in programmes_by_group.get(group.code, [])
            ],
        }
        for group in groups
    ]
    education_group_labels = {
        group.code: {key: value for key, value in {"kk": group.label_kk, "en": group.label_en}.items() if value}
        for group in groups
    }
    programme_labels = {
        programme.code: {
            key: value
            for key, value in {"kk": programme.label_kk, "en": programme.label_en}.items()
            if value
        }
        for programme in programmes
    }

    room_programmes = [
        {"value": row.value, "labels": _labels(row)}
        for row in session.scalars(
            select(ReferenceRoomProgramme)
            .where(ReferenceRoomProgramme.is_active == 1)
            .order_by(ReferenceRoomProgramme.sort_order, ReferenceRoomProgramme.id)
        ).all()
    ]

    return {
        "faculties": faculties,
        "studyLanguages": study_languages,
        "educationGroups": education_groups,
        "educationGroupLabels": education_group_labels,
        "programmeLabels": programme_labels,
        "roomProgrammes": room_programmes,
    }


def get_reference_payload(session: Session | None = None) -> dict[str, object]:
    owns_session = session is None
    active_session = session or SessionLocal()
    try:
        payload = _reference_payload_from_tables(active_session)
        if not any(payload.values()):
            ensure_reference_tables()
            payload = _reference_payload_from_tables(active_session)
        if not any(payload.values()):
            ensure_reference_datasets()
            payload = {**_load_default_references(), **_legacy_reference_payload(active_session)}
        return payload
    finally:
        if owns_session:
            active_session.close()
