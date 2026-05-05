from __future__ import annotations

import os

from redis import Redis
from rq import Worker

from app.core.db import ensure_database
from app.core.monitoring import init_monitoring


def main():
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        raise RuntimeError("REDIS_URL is required to run the RQ schedule worker.")

    init_monitoring()
    ensure_database()

    queue_name = os.getenv("SCHEDULE_RQ_QUEUE", "schedule-generation")
    worker = Worker([queue_name], connection=Redis.from_url(redis_url))
    worker.work()


if __name__ == "__main__":
    main()
