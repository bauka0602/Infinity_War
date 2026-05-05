from __future__ import annotations

import logging
import os

import uvicorn

from app.core.config import HOST
from app.core.db import ensure_database
from app.core.monitoring import init_monitoring


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    init_monitoring()
    ensure_database()
    uvicorn.run("app.api.app:app", host=HOST, port=int(os.getenv("PORT", "8000")))


if __name__ == "__main__":
    main()
