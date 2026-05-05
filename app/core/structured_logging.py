from __future__ import annotations

import json
import logging
from datetime import datetime, timezone


def log_event(logger: logging.Logger, event: str, **fields):
    payload = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
