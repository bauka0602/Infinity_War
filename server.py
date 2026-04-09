import logging
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from app.config import DB_ENGINE, DB_FALLBACK_REASON, DB_FILE, HOST, PORT, REQUESTED_DB_ENGINE
from app.db import ensure_database


def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ensure_database()
    if DB_FALLBACK_REASON:
        logging.warning(DB_FALLBACK_REASON)

    import uvicorn
    from app.fastapi_app import app as fastapi_app

    print(f"Backend started at http://{HOST}:{PORT}")
    if DB_ENGINE == "postgres":
        print("Database engine: PostgreSQL")
    elif REQUESTED_DB_ENGINE == "postgres":
        print(f"Database engine: SQLite fallback ({DB_FILE})")
    else:
        print(f"SQLite database: {DB_FILE}")
    print("HTTP engine: FastAPI")
    uvicorn.run(fastapi_app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    run()
