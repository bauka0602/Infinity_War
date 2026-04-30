from fastapi import APIRouter, Request

from ...admin.service import clear_all_data, clear_collection_data, clear_schedule_data
from ..common import read_json_body

router = APIRouter()


@router.post("/admin/clear-all")
def admin_clear_all(request: Request):
    return clear_all_data(request.headers)


@router.post("/admin/clear/{collection}")
def admin_clear_collection(collection: str, request: Request):
    return clear_collection_data(request.headers, collection)


@router.post("/schedules/reset")
async def schedules_reset(request: Request):
    payload = await read_json_body(request)
    semester = payload.get("semester")
    year = payload.get("year")
    return clear_schedule_data(
        request.headers,
        semester=int(semester) if semester not in (None, "") else None,
        year=int(year) if year not in (None, "") else None,
    )
