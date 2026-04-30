import os


SCHEDULE_ALGORITHMS = {"greedy", "cpsat", "hybrid"}
DEFAULT_SCHEDULE_ALGORITHM = os.getenv("SCHEDULE_ALGORITHM", "greedy").strip().lower()
CP_SAT_SOLVE_SECONDS = float(os.getenv("CP_SAT_SOLVE_SECONDS", "20"))

if DEFAULT_SCHEDULE_ALGORITHM not in SCHEDULE_ALGORITHMS:
    DEFAULT_SCHEDULE_ALGORITHM = "greedy"


def normalize_schedule_algorithm(value):
    algorithm = str(value or DEFAULT_SCHEDULE_ALGORITHM).strip().lower()
    if algorithm in {"cp-sat", "ortools", "or-tools"}:
        return "cpsat"
    if algorithm not in SCHEDULE_ALGORITHMS:
        return DEFAULT_SCHEDULE_ALGORITHM
    return algorithm
