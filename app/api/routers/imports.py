from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...import_service import (
    generate_schedule_export,
    import_iup_data,
    import_rop_data,
    parse_iup_preview,
    parse_rop_preview,
)
from ..common import read_json_body

router = APIRouter()


@router.post("/import/rop/preview")
async def import_rop_preview(request: Request):
    return parse_rop_preview(request.headers, await read_json_body(request))


@router.post("/import/rop")
async def import_rop(request: Request):
    return import_rop_data(request.headers, await read_json_body(request))


@router.post("/import/iup/preview")
async def import_iup_preview(request: Request):
    return parse_iup_preview(request.headers, await read_json_body(request))


@router.post("/import/iup")
async def import_iup(request: Request):
    return import_iup_data(request.headers, await read_json_body(request))


@router.get("/export/schedule")
def export_schedule(request: Request, semester: int | None = None, year: int | None = None):
    export_bytes = generate_schedule_export(request.headers, semester=semester, year=year)
    return Response(
        content=export_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="schedule-export.xlsx"'},
    )

