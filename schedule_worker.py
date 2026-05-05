from __future__ import annotations

import os

from app.core.db import ensure_database
from app.core.monitoring import init_monitoring
from app.schedule.jobs import run_schedule_generation_worker_loop


def main():
    worker_id = os.getenv("SCHEDULE_WORKER_ID") or None
    init_monitoring()
    ensure_database()
    run_schedule_generation_worker_loop(worker_id=worker_id)


if __name__ == "__main__":
    main()
