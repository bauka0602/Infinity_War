from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ScheduleGenerateRequest(BaseModel):
    semester: int = Field(default=1, ge=1, le=2)
    year: int = Field(default=2026, ge=2000, le=2100)
    algorithm: str | None = None


class ScheduleGenerationProgress(BaseModel):
    stage: str
    message: str | None = None
    currentBatch: int | None = None
    totalBatches: int | None = None
    batchSections: int | None = None
    batchPlanItems: int | None = None
    generatedItems: int | None = None
    elapsedSeconds: float | None = None


class ScheduleGenerationJobResponse(BaseModel):
    jobId: str
    status: Literal["queued", "running", "completed", "failed"]
    semester: int
    year: int
    algorithm: str
    createdAt: str
    updatedAt: str
    result: dict[str, Any] | None = None
    error: str | None = None
    errorCode: str | None = None
    details: dict[str, Any] | None = None
    progress: ScheduleGenerationProgress | None = None


class QueueErrorDetails(BaseModel):
    reason: str


class AuthRegisterRequest(BaseModel):
    email: str
    password: str
    displayName: str | None = None
    role: str
    phone: str | None = None
    department: str | None = None
    programmeName: str | None = None
    groupId: int | str | None = None
    subgroup: str | None = None
    language: str | None = None
    teachingLanguages: list[str] | str | None = None


class AuthLoginRequest(BaseModel):
    email: str
    password: str
    role: str | None = None


class TeacherClaimRequest(BaseModel):
    teacherId: int
    email: str | None = None


class TeacherClaimConfirmRequest(BaseModel):
    teacherId: int
    code: str
    password: str
    email: str | None = None


class ProfileAvatarRequest(BaseModel):
    avatarData: str


class ImportFileRequest(BaseModel):
    fileName: str
    fileContent: str
    createMissingCourses: bool | None = None


class TeacherPreferenceCreateRequest(BaseModel):
    preferredDay: str | None = None
    preferred_day: str | None = None
    preferredHour: int | None = None
    preferred_hour: int | None = None
    note: str | None = None


class TeacherPreferenceStatusRequest(BaseModel):
    status: str
    adminComment: str | None = None
    admin_comment: str | None = None


class ScheduleResetRequest(BaseModel):
    semester: int | None = None
    year: int | None = None


class SectionGenerateRequest(BaseModel):
    programme: str | None = None
    studyCourse: int | None = None
    study_course: int | None = None
    semester: int | None = None
    year: int | None = None
