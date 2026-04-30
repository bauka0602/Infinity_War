from fastapi import APIRouter, Request

from ...teachers.preferences import (
    create_teacher_preference_request,
    delete_all_teacher_preference_requests,
    delete_teacher_preference_request,
    list_teacher_preference_requests,
    update_teacher_preference_status,
)
from ..common import read_json_body

router = APIRouter()


@router.get("/teacher-preferences/mine")
def teacher_preferences_mine(request: Request):
    return list_teacher_preference_requests(request.headers, mine=True)


@router.get("/teacher-preferences")
def teacher_preferences_all(request: Request):
    return list_teacher_preference_requests(request.headers, mine=False)


@router.post("/teacher-preferences", status_code=201)
async def teacher_preferences_create(request: Request):
    return create_teacher_preference_request(request.headers, await read_json_body(request))


@router.delete("/teacher-preferences")
def teacher_preferences_delete_all(request: Request):
    return delete_all_teacher_preference_requests(request.headers)


@router.put("/teacher-preferences/{request_id}/status")
async def teacher_preferences_update_status(request_id: int, request: Request):
    return update_teacher_preference_status(request.headers, request_id, await read_json_body(request))


@router.delete("/teacher-preferences/{request_id}")
def teacher_preferences_delete_one(request_id: int, request: Request):
    return delete_teacher_preference_request(request.headers, request_id)
