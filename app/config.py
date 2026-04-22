import os
import threading
from importlib.util import find_spec
from pathlib import Path


def env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_env_file(env_path):
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"

load_env_file(PROJECT_ROOT / ".env")
load_env_file(BASE_DIR / ".env")

DB_FILE = Path(os.getenv("SQLITE_DB_FILE", DATA_DIR / "timetable.db"))
LEGACY_JSON_FILE = DATA_DIR / "store.json"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REQUESTED_DB_ENGINE = (
    "postgres"
    if DATABASE_URL.startswith(("postgres://", "postgresql://"))
    else "sqlite"
)
POSTGRES_DRIVER_AVAILABLE = find_spec("psycopg") is not None
DB_ENGINE = (
    "postgres"
    if REQUESTED_DB_ENGINE == "postgres" and POSTGRES_DRIVER_AVAILABLE
    else "sqlite"
)
DB_FALLBACK_REASON = (
    "DATABASE_URL points to PostgreSQL, but psycopg is not installed. Falling back to SQLite."
    if REQUESTED_DB_ENGINE == "postgres" and not POSTGRES_DRIVER_AVAILABLE
    else ""
)
HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
PORT = int(os.getenv("PORT") or os.getenv("BACKEND_PORT", "8000"))
PASSWORD_PREFIX = "sha256$"
TEACHER_EMAIL_DOMAIN = "@kazatu.edu.kz"
EXPOSE_DEV_CLAIM_CODE = env_flag("EXPOSE_DEV_CLAIM_CODE", False)

raw_allowed_origins = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://localhost:4173,http://127.0.0.1:4173",
)
ALLOWED_ORIGINS = [
    origin.strip() for origin in raw_allowed_origins.split(",") if origin.strip()
]

DB_LOCK = threading.Lock()
