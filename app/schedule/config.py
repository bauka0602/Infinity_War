import os


SCHEDULE_ALGORITHMS = {"greedy", "cpsat", "cpsat_fast", "hybrid"}
DEFAULT_SCHEDULE_ALGORITHM = os.getenv("SCHEDULE_ALGORITHM", "cpsat").strip().lower()
CP_SAT_SOLVE_SECONDS = float(os.getenv("CP_SAT_SOLVE_SECONDS", "120"))
CP_SAT_MAX_ROOM_CANDIDATES = int(os.getenv("CP_SAT_MAX_ROOM_CANDIDATES", "8"))
CP_SAT_RELATIVE_GAP_LIMIT = float(os.getenv("CP_SAT_RELATIVE_GAP_LIMIT", "0.03"))
CP_SAT_NUM_WORKERS = int(os.getenv("CP_SAT_NUM_WORKERS", "8"))
CP_SAT_WARM_START_ENABLED = os.getenv("CP_SAT_WARM_START_ENABLED", "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

if DEFAULT_SCHEDULE_ALGORITHM not in SCHEDULE_ALGORITHMS:
    DEFAULT_SCHEDULE_ALGORITHM = "cpsat"


def normalize_schedule_algorithm(value):
    algorithm = str(value or DEFAULT_SCHEDULE_ALGORITHM).strip().lower()
    if algorithm in {"cp-sat", "ortools", "or-tools"}:
        return "cpsat"
    if algorithm in {"cp-sat-fast", "cpsat-fast", "cpsat fast", "fast_cpsat"}:
        return "cpsat_fast"
    if algorithm not in SCHEDULE_ALGORITHMS:
        return DEFAULT_SCHEDULE_ALGORITHM
    return algorithm
