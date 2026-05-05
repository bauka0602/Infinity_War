from __future__ import annotations

import logging
import os
from pathlib import Path

from alembic import command
from alembic.config import Config

LOGGER = logging.getLogger(__name__)


def _truthy(value):
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def run_startup_migrations():
    if not _truthy(os.getenv("ALEMBIC_AUTO_UPGRADE", "true")):
        LOGGER.info("Alembic startup migrations are disabled.")
        return

    backend_dir = Path(__file__).resolve().parents[2]
    alembic_ini = backend_dir / "alembic.ini"
    if not alembic_ini.exists():
        LOGGER.warning("Alembic config was not found at %s", alembic_ini)
        return

    config = Config(str(alembic_ini))
    LOGGER.info("Applying Alembic migrations.")
    command.upgrade(config, "head")
