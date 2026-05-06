from copy import deepcopy

from ..config import CP_SAT_MAX_ROOM_CANDIDATES, CP_SAT_NUM_WORKERS, CP_SAT_SOLVE_SECONDS
from .cp_sat_optimizer import optimize_schedule


def _fast_cpsat_payload(payload):
    optimized = deepcopy(payload)
    optimized["maxSolveTimeSeconds"] = CP_SAT_SOLVE_SECONDS
    optimized["maxRoomCandidatesPerItem"] = CP_SAT_MAX_ROOM_CANDIDATES
    optimized["numWorkers"] = CP_SAT_NUM_WORKERS
    optimized["stopAfterFirstSolution"] = True
    optimized["relativeGapLimit"] = 0
    optimized["enableGapPenalties"] = False
    optimized["enableBuildingTransitionPenalties"] = False
    optimized["preferSeparateSubgroupsByDay"] = False
    optimized["maxClassesPerDayForTeacher"] = max(
        6,
        int(optimized.get("maxClassesPerDayForTeacher") or 0),
    )
    optimized["maxClassesPerDayForAudience"] = max(
        6,
        int(optimized.get("maxClassesPerDayForAudience") or 0),
    )
    return optimized


def optimize_cpsat_fast_schedule(payload, context=None):
    result = optimize_schedule(_fast_cpsat_payload(payload))
    diagnostics = result.setdefault("diagnostics", {})
    diagnostics["relaxedRetryUsed"] = False
    diagnostics["cpsatMode"] = "fast-first-feasible"
    return result
