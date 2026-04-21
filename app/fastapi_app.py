import json
import logging
import sqlite3
from datetime import date

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from .admin_service import clear_all_data, clear_collection_data
from .auth_service import (
    confirm_teacher_claim,
    get_current_profile,
    login_user,
    logout_user,
    register_user,
    request_teacher_claim,
    require_auth_user,
    search_claimable_teachers,
    update_profile_avatar,
)
from .collections import (
    create_collection_item,
    delete_collection_item,
    list_collection,
    update_collection_item,
)
from .config import ALLOWED_ORIGINS, DB_ENGINE, DB_LOCK
from .db import get_connection, query_all, query_one
from .errors import ApiError
from .import_service import (
    generate_import_template,
    generate_schedule_export,
    import_excel_data,
    import_rop_data,
    parse_rop_preview,
)
from .job_store import create_schedule_generation_job, get_schedule_generation_job
from .notification_service import (
    create_schedule_change_notifications,
    delete_all_notifications,
    delete_notification,
    list_notifications,
    mark_all_notifications_as_read,
    mark_notification_as_read,
)
from .preference_service import (
    create_teacher_preference_request,
    delete_all_teacher_preference_requests,
    delete_teacher_preference_request,
    list_teacher_preference_requests,
    update_teacher_preference_status,
)

LOGGER = logging.getLogger(__name__)

COLLECTION_ALIASES = {
    "disciplines": "courses",
}
ALLOWED_COLLECTIONS = {
    "courses",
    "course_components",
    "teachers",
    "students",
    "rooms",
    "groups",
    "schedules",
    "sections",
}


def _json_error(status, message, code, details=None):
    payload = {"error": message, "errorCode": code}
    if details:
        payload["details"] = details
    return JSONResponse(status_code=status, content=payload)


def _query_params_to_legacy_dict(request: Request):
    result = {}
    for key, value in request.query_params.multi_items():
        result.setdefault(key, []).append(value)
    return result


async def _read_json_body(request: Request):
    body = await request.body()
    if not body:
        return {}
    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ApiError(400, "invalid_json", "Некорректный JSON") from exc


def _require_collection_access(collection, headers, method):
    user = require_auth_user(headers)

    if collection in {"courses", "course_components", "teachers", "students", "rooms", "groups", "sections"} and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    if collection == "schedules" and method in {"POST", "PUT", "DELETE"} and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    return user


def _resolve_collection(raw_collection):
    collection = COLLECTION_ALIASES.get(raw_collection, raw_collection)
    if collection not in ALLOWED_COLLECTIONS:
        raise ApiError(404, "not_found", "Not found")
    return collection


def create_app():
    app = FastAPI(title="TimeTableG API", version="3.0.0")

    allow_origins = ["*"] if "*" in ALLOWED_ORIGINS else ALLOWED_ORIGINS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    )

    @app.exception_handler(ApiError)
    async def api_error_handler(_request: Request, exc: ApiError):
        return _json_error(exc.status, exc.message, exc.code, exc.details or None)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(_request: Request, _exc: RequestValidationError):
        return _json_error(400, "Некорректный запрос", "bad_request")

    @app.exception_handler(json.JSONDecodeError)
    async def json_decode_handler(_request: Request, _exc: json.JSONDecodeError):
        return _json_error(400, "Некорректный JSON", "invalid_json")

    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, _exc: ValueError):
        return _json_error(400, "Некорректный запрос", "bad_request")

    @app.exception_handler(sqlite3.IntegrityError)
    async def sqlite_integrity_handler(_request: Request, _exc: sqlite3.IntegrityError):
        return _json_error(400, "Ошибка базы данных", "database_error")

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception):
        if exc.__class__.__name__ == "UniqueViolation":
            return _json_error(400, "Ошибка базы данных", "database_error")
        LOGGER.exception("Unhandled API error for %s %s", request.method, request.url.path)
        return _json_error(500, "Внутренняя ошибка сервера", "internal_server_error")

    router = APIRouter(prefix="/api")

    @router.get("/health")
    def health():
        return {"status": "ok", "engine": DB_ENGINE}

    @router.post("/auth/register", status_code=201)
    async def auth_register(request: Request):
        return register_user(await _read_json_body(request))

    @router.post("/auth/teacher-claim/request")
    async def auth_teacher_claim_request(request: Request):
        return request_teacher_claim(await _read_json_body(request))

    @router.post("/auth/teacher-claim/confirm")
    async def auth_teacher_claim_confirm(request: Request):
        return confirm_teacher_claim(await _read_json_body(request))

    @router.post("/auth/login")
    async def auth_login(request: Request):
        return login_user(await _read_json_body(request))

    @router.post("/auth/logout")
    def auth_logout(request: Request):
        return logout_user(request.headers)

    @router.get("/profile")
    def profile_get(request: Request):
        return get_current_profile(request.headers)

    @router.post("/profile/avatar")
    async def profile_avatar(request: Request):
        return update_profile_avatar(request.headers, await _read_json_body(request))

    @router.get("/notifications")
    def notifications_get(request: Request):
        return list_notifications(request.headers)

    @router.post("/notifications/read-all")
    def notifications_read_all(request: Request):
        return mark_all_notifications_as_read(request.headers)

    @router.delete("/notifications")
    def notifications_delete_all(request: Request):
        return delete_all_notifications(request.headers)

    @router.put("/notifications/{notification_id}/read")
    def notifications_read_one(notification_id: int, request: Request):
        return mark_notification_as_read(request.headers, notification_id)

    @router.delete("/notifications/{notification_id}")
    def notifications_delete_one(notification_id: int, request: Request):
        return delete_notification(request.headers, notification_id)

    @router.get("/teacher-preferences/mine")
    def teacher_preferences_mine(request: Request):
        return list_teacher_preference_requests(request.headers, mine=True)

    @router.get("/teacher-preferences")
    def teacher_preferences_all(request: Request):
        return list_teacher_preference_requests(request.headers, mine=False)

    @router.post("/teacher-preferences", status_code=201)
    async def teacher_preferences_create(request: Request):
        return create_teacher_preference_request(request.headers, await _read_json_body(request))

    @router.delete("/teacher-preferences")
    def teacher_preferences_delete_all(request: Request):
        return delete_all_teacher_preference_requests(request.headers)

    @router.put("/teacher-preferences/{request_id}/status")
    async def teacher_preferences_update_status(request_id: int, request: Request):
        return update_teacher_preference_status(request.headers, request_id, await _read_json_body(request))

    @router.delete("/teacher-preferences/{request_id}")
    def teacher_preferences_delete_one(request_id: int, request: Request):
        return delete_teacher_preference_request(request.headers, request_id)

    @router.get("/public/groups")
    def public_groups():
        with DB_LOCK:
            with get_connection() as connection:
                return query_all(
                    connection,
                    """
                    SELECT id, name, student_count, has_subgroups, language, programme, specialty_code, entry_year, study_course
                    FROM groups
                    ORDER BY name, id
                    """,
                )

    @router.get("/public/teachers/claim-search")
    def public_teachers_claim_search(q: str = ""):
        return search_claimable_teachers(q)

    @router.post("/import/excel")
    async def import_excel(request: Request):
        return import_excel_data(request.headers, await _read_json_body(request))

    @router.post("/import/rop/preview")
    async def import_rop_preview(request: Request):
        return parse_rop_preview(request.headers, await _read_json_body(request))

    @router.post("/import/rop")
    async def import_rop(request: Request):
        return import_rop_data(request.headers, await _read_json_body(request))

    @router.get("/import/template")
    def import_template(request: Request):
        template_bytes = generate_import_template(request.headers)
        return Response(
            content=template_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="timetable-import-template.xlsx"'},
        )

    @router.get("/export/schedule")
    def export_schedule(request: Request):
        export_bytes = generate_schedule_export(request.headers)
        return Response(
            content=export_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="schedule-export.xlsx"'},
        )

    @router.post("/admin/clear-all")
    def admin_clear_all(request: Request):
        return clear_all_data(request.headers)

    @router.post("/admin/clear/{collection}")
    def admin_clear_collection(collection: str, request: Request):
        return clear_collection_data(request.headers, collection)

    @router.post("/schedules/generate", status_code=202)
    async def schedule_generate(request: Request):
        user = require_auth_user(request.headers)
        if user["role"] != "admin":
            raise ApiError(403, "forbidden", "Недостаточно прав")

        payload = await _read_json_body(request)
        semester = int(payload.get("semester") or 1)
        year = int(payload.get("year") or date.today().year)
        algorithm = payload.get("algorithm") or "greedy"
        return create_schedule_generation_job(semester, year, algorithm)

    @router.get("/schedules/generate/{job_id}")
    def schedule_generate_status(job_id: str, request: Request):
        user = require_auth_user(request.headers)
        if user["role"] != "admin":
            raise ApiError(403, "forbidden", "Недостаточно прав")
        return get_schedule_generation_job(job_id)

    @router.get("/{raw_collection}")
    def collection_list(raw_collection: str, request: Request):
        collection = _resolve_collection(raw_collection)
        user = _require_collection_access(collection, request.headers, "GET")
        query = _query_params_to_legacy_dict(request)
        with DB_LOCK:
            with get_connection() as connection:
                return list_collection(connection, collection, query, user)

    @router.post("/{raw_collection}", status_code=201)
    async def collection_create(raw_collection: str, request: Request):
        collection = _resolve_collection(raw_collection)
        _require_collection_access(collection, request.headers, "POST")
        payload = await _read_json_body(request)
        with DB_LOCK:
            with get_connection() as connection:
                created = create_collection_item(connection, collection, payload)
                if collection == "schedules":
                    create_schedule_change_notifications(connection, after_item=created)
                return created

    @router.put("/{raw_collection}/{item_id}")
    async def collection_update(raw_collection: str, item_id: int, request: Request):
        collection = _resolve_collection(raw_collection)
        _require_collection_access(collection, request.headers, "PUT")
        payload = await _read_json_body(request)
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
        collection = _resolve_collection(raw_collection)
        _require_collection_access(collection, request.headers, "DELETE")
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

    app.include_router(router)
    return app


app = create_app()
