from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...imports.service import (
    generate_schedule_export,
    import_iup_data,
    import_rop_data,
    parse_iup_preview,
    parse_rop_preview,
)
from ..schemas import ImportFileRequest

router = APIRouter()


@router.post("/import/rop/preview")
async def import_rop_preview(payload: ImportFileRequest, request: Request):
    return parse_rop_preview(request.headers, payload.model_dump(exclude_none=True))


@router.post("/import/rop")
async def import_rop(payload: ImportFileRequest, request: Request):
    return import_rop_data(request.headers, payload.model_dump(exclude_none=True))


@router.post("/import/iup/preview")
async def import_iup_preview(payload: ImportFileRequest, request: Request):
    return parse_iup_preview(request.headers, payload.model_dump(exclude_none=True))


@router.post("/import/iup")
async def import_iup(payload: ImportFileRequest, request: Request):
    return import_iup_data(request.headers, payload.model_dump(exclude_none=True))


@router.get("/export/schedule")
def export_schedule(
    request: Request,
    semester: int | None = None,
    year: int | None = None,
    language: str | None = None,
    group_id: int | None = None,
):
    export_bytes = generate_schedule_export(
        request.headers,
        semester=semester,
        year=year,
        language=language,
        group_id=group_id,
    )
    return Response(
        content=export_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="schedule-export.xlsx"'},
    )
