import json
import logging
import sqlite3

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from .api.common import json_error
from .api.routers import admin, auth, collections, imports, notifications, public, schedules, system, teacher_preferences
from .config import ALLOWED_ORIGINS
from .errors import ApiError

LOGGER = logging.getLogger(__name__)


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
        return json_error(exc.status, exc.message, exc.code, exc.details or None)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(_request: Request, _exc: RequestValidationError):
        return json_error(400, "Некорректный запрос", "bad_request")

    @app.exception_handler(json.JSONDecodeError)
    async def json_decode_handler(_request: Request, _exc: json.JSONDecodeError):
        return json_error(400, "Некорректный JSON", "invalid_json")

    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, _exc: ValueError):
        return json_error(400, "Некорректный запрос", "bad_request")

    @app.exception_handler(sqlite3.IntegrityError)
    async def sqlite_integrity_handler(_request: Request, _exc: sqlite3.IntegrityError):
        return json_error(400, "Ошибка базы данных", "database_error")

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception):
        if exc.__class__.__name__ == "UniqueViolation":
            return json_error(400, "Ошибка базы данных", "database_error")
        LOGGER.exception("Unhandled API error for %s %s", request.method, request.url.path)
        return json_error(500, "Внутренняя ошибка сервера", "internal_server_error")

    for router_module in (
        system,
        auth,
        notifications,
        teacher_preferences,
        public,
        imports,
        admin,
        schedules,
        collections,
    ):
        app.include_router(router_module.router, prefix="/api")
    return app


app = create_app()
