from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from sqlalchemy import select

from ...auth.service import require_auth_user
from ...core.config import DB_LOCK
from ...core.errors import ApiError
from ...core.orm import SessionLocal
from ...models import (
    ReferenceEducationGroup,
    ReferenceEducationalProgramme,
    ReferenceFaculty,
    ReferenceRoomProgramme,
    ReferenceStudyLanguage,
)
from ...reference.service import get_reference_payload

router = APIRouter()


REFERENCE_DATASETS = {
    "faculties": {
        "model": ReferenceFaculty,
        "fields": ["value", "label_ru", "label_kk", "label_en", "sort_order", "is_active"],
        "required": ["value", "label_ru"],
    },
    "study-languages": {
        "model": ReferenceStudyLanguage,
        "fields": ["value", "label_key", "sort_order", "is_active"],
        "required": ["value", "label_key"],
    },
    "education-groups": {
        "model": ReferenceEducationGroup,
        "fields": ["code", "label_ru", "label_kk", "label_en", "sort_order", "is_active"],
        "required": ["code", "label_ru"],
    },
    "programmes": {
        "model": ReferenceEducationalProgramme,
        "fields": [
            "code",
            "education_group_code",
            "label_ru",
            "label_kk",
            "label_en",
            "sort_order",
            "is_active",
        ],
        "required": ["code", "education_group_code", "label_ru"],
    },
    "room-programmes": {
        "model": ReferenceRoomProgramme,
        "fields": ["value", "label_ru", "label_kk", "label_en", "sort_order", "is_active"],
        "required": ["value", "label_ru"],
    },
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_admin(request: Request) -> dict[str, Any]:
    user = require_auth_user(request.headers)
    if user.get("role") != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return user


def _get_dataset_config(dataset: str) -> dict[str, Any]:
    config = REFERENCE_DATASETS.get(dataset)
    if config is None:
        raise ApiError(404, "not_found", "Справочник не найден")
    return config


def _normalize_payload(payload: dict[str, Any], fields: list[str], required: list[str]) -> dict[str, Any]:
    missing = [field for field in required if not str(payload.get(field) or "").strip()]
    if missing:
        raise ApiError(
            400,
            "fill_required_fields",
            f"Заполните поля: {', '.join(missing)}",
            {"fields": missing},
        )

    normalized: dict[str, Any] = {}
    for field in fields:
        if field not in payload:
            continue
        value = payload.get(field)
        if field == "sort_order":
            try:
                normalized[field] = int(value or 0)
            except (TypeError, ValueError) as exc:
                raise ApiError(400, "bad_request", "Порядок должен быть числом") from exc
        elif field == "is_active":
            normalized[field] = 1 if value else 0
        elif value is None:
            normalized[field] = None
        else:
            normalized[field] = str(value).strip()
    return normalized


def _serialize_reference_item(item: Any, fields: list[str]) -> dict[str, Any]:
    return {
        "id": item.id,
        **{field: getattr(item, field) for field in fields},
        "updated_at": item.updated_at,
    }


@router.get("/reference-data")
def reference_data():
    with DB_LOCK:
        return get_reference_payload()


@router.get("/reference-data/{dataset}")
def reference_dataset_list(dataset: str, request: Request):
    _require_admin(request)
    config = _get_dataset_config(dataset)
    model = config["model"]
    fields = config["fields"]
    with DB_LOCK, SessionLocal() as session:
        rows = session.scalars(select(model).order_by(model.sort_order, model.id)).all()
        return [_serialize_reference_item(row, fields) for row in rows]


@router.post("/reference-data/{dataset}", status_code=201)
def reference_dataset_create(dataset: str, payload: dict[str, Any], request: Request):
    _require_admin(request)
    config = _get_dataset_config(dataset)
    model = config["model"]
    fields = config["fields"]
    data = _normalize_payload(payload, fields, config["required"])
    data.setdefault("sort_order", 0)
    data.setdefault("is_active", 1)
    data["updated_at"] = _utc_now()
    with DB_LOCK, SessionLocal() as session:
        item = model(**data)
        session.add(item)
        session.commit()
        session.refresh(item)
        return _serialize_reference_item(item, fields)


@router.put("/reference-data/{dataset}/{item_id}")
def reference_dataset_update(
    dataset: str,
    item_id: int,
    payload: dict[str, Any],
    request: Request,
):
    _require_admin(request)
    config = _get_dataset_config(dataset)
    model = config["model"]
    fields = config["fields"]
    data = _normalize_payload(payload, fields, config["required"])
    with DB_LOCK, SessionLocal() as session:
        item = session.get(model, item_id)
        if item is None:
            raise ApiError(404, "record_not_found", "Запись не найдена")
        for field, value in data.items():
            setattr(item, field, value)
        item.updated_at = _utc_now()
        session.commit()
        session.refresh(item)
        return _serialize_reference_item(item, fields)


@router.delete("/reference-data/{dataset}/{item_id}")
def reference_dataset_delete(dataset: str, item_id: int, request: Request):
    _require_admin(request)
    config = _get_dataset_config(dataset)
    model = config["model"]
    fields = config["fields"]
    with DB_LOCK, SessionLocal() as session:
        item = session.get(model, item_id)
        if item is None:
            raise ApiError(404, "record_not_found", "Запись не найдена")
        item.is_active = 0
        item.updated_at = _utc_now()
        session.commit()
        session.refresh(item)
        return _serialize_reference_item(item, fields)
