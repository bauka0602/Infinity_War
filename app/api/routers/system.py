from fastapi import APIRouter

from ...core.config import DB_ENGINE

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok", "engine": DB_ENGINE}
