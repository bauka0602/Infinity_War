from fastapi import APIRouter, Request

from ...collections.service import (
    create_collection_item,
    delete_collection_item,
    list_collection,
    update_collection_item,
)
from ...core.config import DB_LOCK
from ...core.db import get_connection, query_one
from ...core.errors import ApiError
from ...notifications.service import create_schedule_change_notifications
from ..common import (
    query_params_to_legacy_dict,
    read_json_body,
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
        with get_connection() as connection:
            return list_collection(connection, collection, query, user)


@router.post("/{raw_collection}", status_code=201)
async def collection_create(raw_collection: str, request: Request):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "POST")
    payload = await read_json_body(request)
    with DB_LOCK:
        with get_connection() as connection:
            created = create_collection_item(connection, collection, payload)
            if collection == "schedules":
                create_schedule_change_notifications(connection, after_item=created)
            return created


@router.put("/{raw_collection}/{item_id}")
async def collection_update(raw_collection: str, item_id: int, request: Request):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "PUT")
    payload = await read_json_body(request)
    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(connection, f"SELECT * FROM {collection} WHERE id = ?", (item_id,))
            if existing is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            updated = update_collection_item(connection, collection, item_id, payload)
            if collection == "schedules":
                create_schedule_change_notifications(connection, before_item=existing, after_item=updated)
            return updated


@router.delete("/{raw_collection}/{item_id}")
def collection_delete(raw_collection: str, item_id: int, request: Request):
    collection = resolve_collection(raw_collection)
    require_collection_access(collection, request.headers, "DELETE")
    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(connection, f"SELECT * FROM {collection} WHERE id = ?", (item_id,))
            if existing is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            if collection == "sections":
                raise ApiError(405, "method_not_allowed", "Удаление секций недоступно")
            delete_collection_item(connection, collection, item_id)
            if collection == "schedules":
                create_schedule_change_notifications(connection, before_item=existing)
            return {"success": True}
