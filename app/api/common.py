from fastapi import Request
from fastapi.responses import JSONResponse

from ..auth.service import require_auth_user
from ..core.errors import ApiError

COLLECTION_ALIASES = {
    "disciplines": "courses",
}

ALLOWED_COLLECTIONS = {
    "courses",
    "course_components",
    "iup_entries",
    "teachers",
    "students",
    "rooms",
    "room_blocks",
    "groups",
    "schedules",
    "sections",
}


def json_error(status, message, code, details=None):
    payload = {"error": message, "errorCode": code}
    if details:
        payload["details"] = details
    return JSONResponse(status_code=status, content=payload)


def query_params_to_legacy_dict(request: Request):
    result = {}
    for key, value in request.query_params.multi_items():
        result.setdefault(key, []).append(value)
    return result


def require_collection_access(collection, headers, method):
    user = require_auth_user(headers)

    if collection in {
        "courses",
        "course_components",
        "iup_entries",
        "teachers",
        "students",
        "rooms",
        "room_blocks",
        "groups",
        "sections",
    } and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    if collection == "schedules" and method in {"POST", "PUT", "DELETE"} and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")

    return user


def resolve_collection(raw_collection):
    collection = COLLECTION_ALIASES.get(raw_collection, raw_collection)
    if collection not in ALLOWED_COLLECTIONS:
        raise ApiError(404, "not_found", "Not found")
    return collection
