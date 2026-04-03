import json
import sqlite3
from datetime import date
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .auth_service import login_user, register_user, require_auth_user
from .collections import create_collection_item, delete_collection_item, list_collection, update_collection_item
from .config import ALLOWED_ORIGINS, DB_ENGINE, DB_LOCK
from .db import get_connection, query_one
from .errors import ApiError
from .scheduling import build_schedule


def resolve_allowed_origin(request_origin):
    if "*" in ALLOWED_ORIGINS:
        return "*"
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return None


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "TimeTableGBackend/3.0"

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_headers()
        self.end_headers()

    def do_GET(self):
        self.route_request("GET")

    def do_POST(self):
        self.route_request("POST")

    def do_PUT(self):
        self.route_request("PUT")

    def do_DELETE(self):
        self.route_request("DELETE")

    def _set_headers(self, content_type="application/json"):
        allowed_origin = resolve_allowed_origin(self.headers.get("Origin"))
        self.send_header("Content-Type", content_type)
        if allowed_origin:
            self.send_header("Access-Control-Allow-Origin", allowed_origin)
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        if allowed_origin and allowed_origin != "*":
            self.send_header("Vary", "Origin")

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._set_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}
        raw_body = self.rfile.read(content_length).decode("utf-8")
        return json.loads(raw_body) if raw_body else {}

    def route_request(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if not path.startswith("/api"):
            self.send_json(404, {"error": "Not found", "errorCode": "not_found"})
            return

        api_path = path[4:] or "/"

        try:
            if api_path == "/health" and method == "GET":
                self.send_json(200, {"status": "ok", "engine": DB_ENGINE})
                return

            if api_path == "/auth/register" and method == "POST":
                self.send_json(201, register_user(self.read_json()))
                return

            if api_path == "/auth/login" and method == "POST":
                self.send_json(200, login_user(self.read_json()))
                return

            if api_path == "/auth/logout" and method == "POST":
                self.send_json(200, {"success": True})
                return

            if api_path == "/schedules/generate" and method == "POST":
                self.handle_schedule_generation()
                return

            self.handle_collection_routes(method, api_path, parse_qs(parsed.query))
        except ApiError as exc:
            payload = {"error": exc.message, "errorCode": exc.code}
            if exc.details:
                payload["details"] = exc.details
            self.send_json(exc.status, payload)
        except ValueError:
            self.send_json(400, {"error": "Некорректный запрос", "errorCode": "bad_request"})
        except json.JSONDecodeError:
            self.send_json(400, {"error": "Некорректный JSON", "errorCode": "invalid_json"})
        except (sqlite3.IntegrityError, Exception) as exc:
            is_integrity = exc.__class__.__name__ in {"IntegrityError", "UniqueViolation"}
            if is_integrity:
                self.send_json(400, {"error": "Ошибка базы данных", "errorCode": "database_error"})
                return
            self.send_json(500, {"error": "Внутренняя ошибка сервера", "errorCode": "internal_server_error"})

    def handle_collection_routes(self, method, api_path, query):
        parts = [part for part in api_path.split("/") if part]
        if not parts:
            self.send_json(404, {"error": "Not found", "errorCode": "not_found"})
            return

        collection = parts[0]
        if collection not in {"courses", "teachers", "rooms", "schedules"}:
            self.send_json(404, {"error": "Not found", "errorCode": "not_found"})
            return

        try:
            user = require_auth_user(self.headers)
        except ApiError as exc:
            self.send_json(exc.status, {"error": exc.message, "errorCode": exc.code})
            return

        if collection in {"courses", "teachers", "rooms"} and user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав", "errorCode": "forbidden"})
            return

        if collection == "schedules" and method in {"POST", "PUT", "DELETE"} and user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав", "errorCode": "forbidden"})
            return

        with DB_LOCK:
            with get_connection() as connection:
                if len(parts) == 1:
                    if method == "GET":
                        self.send_json(200, list_collection(connection, collection, query))
                        return

                    if method == "POST":
                        created = create_collection_item(connection, collection, self.read_json())
                        self.send_json(201, created)
                        return

                if len(parts) == 2:
                    try:
                        item_id = int(parts[1])
                    except ValueError as exc:
                        raise ApiError(400, "invalid_id", "ID должен быть числом") from exc

                    existing = query_one(
                        connection,
                        f"SELECT id FROM {collection} WHERE id = ?",
                        (item_id,),
                    )
                    if existing is None:
                        self.send_json(404, {"error": "Запись не найдена", "errorCode": "record_not_found"})
                        return

                    if method == "PUT":
                        updated = update_collection_item(
                            connection,
                            collection,
                            item_id,
                            self.read_json(),
                        )
                        self.send_json(200, updated)
                        return

                    if method == "DELETE":
                        delete_collection_item(connection, collection, item_id)
                        self.send_json(200, {"success": True})
                        return

        self.send_json(405, {"error": "Method not allowed", "errorCode": "method_not_allowed"})

    def handle_schedule_generation(self):
        try:
            user = require_auth_user(self.headers)
        except ApiError as exc:
            self.send_json(exc.status, {"error": exc.message, "errorCode": exc.code})
            return

        if user["role"] != "admin":
            self.send_json(403, {"error": "Недостаточно прав", "errorCode": "forbidden"})
            return

        payload = self.read_json()
        semester = int(payload.get("semester") or 1)
        year = int(payload.get("year") or date.today().year)
        algorithm = payload.get("algorithm") or "greedy"

        with DB_LOCK:
            with get_connection() as connection:
                generated = build_schedule(connection, semester, year, algorithm)

        self.send_json(200, generated)
