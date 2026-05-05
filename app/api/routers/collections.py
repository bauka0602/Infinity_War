from typing import Any

from fastapi import APIRouter, Request

from ...collections.service import (
    create_collection_item,
    delete_collection_item,
    get_collection_item,
    list_collection,
    update_collection_item,
)
from ...core.config import DB_LOCK
from ...core.errors import ApiError
from ...notifications.service import create_schedule_change_notifications
from ..common import (
    query_params_to_legacy_dict,
    require_collection_access,
    resolve_collection,
)

router = APIRouter()


@router.get("/{raw_collection}")
def collection_list(raw_collection: str, request: Request):
    collection = resolve_collection(raw_collection)
    user = require_collection_access(collection, request.headers, "GET")
    query = query_params_to_legacy_dict(request)
    with DB_LOCK:
        return list_collection(None, collection, query, user)


@router.post("/{raw_collection}", status_code=201)
async def collection_create(raw_collection: str, payload: dict[str, Any], request: Request):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "POST")
    with DB_LOCK:
        created = create_collection_item(None, collection, payload)
        if collection == "schedules":
            create_schedule_change_notifications(None, after_item=created)
        return created


@router.put("/{raw_collection}/{item_id}")
async def collection_update(
    raw_collection: str,
    item_id: int,
    payload: dict[str, Any],
    request: Request,
):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "PUT")
    with DB_LOCK:
        existing = get_collection_item(collection, item_id)
        if existing is None:
            raise ApiError(404, "record_not_found", "Запись не найдена")
        updated = update_collection_item(None, collection, item_id, payload)
        if collection == "schedules":
            create_schedule_change_notifications(None, before_item=existing, after_item=updated)
        return updated


@router.delete("/{raw_collection}/{item_id}")
def collection_delete(raw_collection: str, item_id: int, request: Request):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "DELETE")
    with DB_LOCK:
        existing = get_collection_item(collection, item_id)
        if existing is None:
            raise ApiError(404, "record_not_found", "Запись не найдена")
        if collection == "sections":
            raise ApiError(405, "method_not_allowed", "Удаление секций недоступно")
        delete_collection_item(None, collection, item_id)
        if collection == "schedules":
            create_schedule_change_notifications(None, before_item=existing)
        return {"success": True}
