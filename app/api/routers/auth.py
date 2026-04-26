from fastapi import APIRouter, Request

from ...auth_service import (
    confirm_teacher_claim,
    get_current_profile,
    login_user,
    logout_user,
    register_user,
    request_teacher_claim,
    update_profile_avatar,
)
from ..common import read_json_body

router = APIRouter()


@router.post("/auth/register", status_code=201)
async def auth_register(request: Request):
    return register_user(await read_json_body(request))


@router.post("/auth/teacher-claim/request")
async def auth_teacher_claim_request(request: Request):
    return request_teacher_claim(await read_json_body(request))


@router.post("/auth/teacher-claim/confirm")
async def auth_teacher_claim_confirm(request: Request):
    return confirm_teacher_claim(await read_json_body(request))


@router.post("/auth/login")
async def auth_login(request: Request):
    return login_user(await read_json_body(request))


@router.post("/auth/logout")
def auth_logout(request: Request):
    return logout_user(request.headers)


@router.get("/profile")
def profile_get(request: Request):
    return get_current_profile(request.headers)


@router.post("/profile/avatar")
async def profile_avatar(request: Request):
    return update_profile_avatar(request.headers, await read_json_body(request))

