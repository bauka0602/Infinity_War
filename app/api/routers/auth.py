from fastapi import APIRouter, Request

from ...auth.service import (
    confirm_teacher_claim,
    get_current_profile,
    login_user,
    logout_user,
    register_user,
    request_teacher_claim,
    update_profile_avatar,
    update_profile_email,
)
from ..schemas import (
    AuthLoginRequest,
    AuthRegisterRequest,
    ProfileAvatarRequest,
    ProfileEmailRequest,
    TeacherClaimConfirmRequest,
    TeacherClaimRequest,
)

router = APIRouter()


@router.post("/auth/register", status_code=201)
async def auth_register(payload: AuthRegisterRequest):
    return register_user(payload.model_dump(exclude_none=True))


@router.post("/auth/teacher-claim/request")
async def auth_teacher_claim_request(payload: TeacherClaimRequest):
    return request_teacher_claim(payload.model_dump(exclude_none=True))


@router.post("/auth/teacher-claim/confirm")
async def auth_teacher_claim_confirm(payload: TeacherClaimConfirmRequest):
    return confirm_teacher_claim(payload.model_dump(exclude_none=True))


@router.post("/auth/login")
async def auth_login(payload: AuthLoginRequest):
    return login_user(payload.model_dump(exclude_none=True))


@router.post("/auth/logout")
def auth_logout(request: Request):
    return logout_user(request.headers)


@router.get("/profile")
def profile_get(request: Request):
    return get_current_profile(request.headers)


@router.post("/profile/avatar")
async def profile_avatar(payload: ProfileAvatarRequest, request: Request):
    return update_profile_avatar(request.headers, payload.model_dump(exclude_none=True))


@router.put("/profile/email")
async def profile_email(payload: ProfileEmailRequest, request: Request):
    return update_profile_email(request.headers, payload.model_dump(exclude_none=True))
