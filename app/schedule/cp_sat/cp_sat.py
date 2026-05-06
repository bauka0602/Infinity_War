from copy import deepcopy

from ...core.errors import ApiError
from ..config import (
    CP_SAT_MAX_ROOM_CANDIDATES,
    CP_SAT_NUM_WORKERS,
    CP_SAT_RELATIVE_GAP_LIMIT,
    CP_SAT_SOLVE_SECONDS,
)
from .cp_sat_optimizer import optimize_schedule


def _batch_error_details(exc, context, payload):
    details = getattr(exc, "details", None) or {}
    if not isinstance(details, dict):
        details = {}
    return {
        **details,
        "studyCourse": context.get("study_course"),
        "batchKey": context.get("batch_key"),
        "batchSections": context.get("batch_sections_count"),
        "batchPlanItems": len(payload.get("planItems") or []),
    }


def _quality_cpsat_payload(payload):
    optimized = deepcopy(payload)
    optimized["maxSolveTimeSeconds"] = CP_SAT_SOLVE_SECONDS
    optimized["maxRoomCandidatesPerItem"] = CP_SAT_MAX_ROOM_CANDIDATES
    optimized["numWorkers"] = CP_SAT_NUM_WORKERS
    optimized["stopAfterFirstSolution"] = False
    optimized["relativeGapLimit"] = CP_SAT_RELATIVE_GAP_LIMIT
    optimized.setdefault("enableGapPenalties", True)
    optimized.setdefault("enableBuildingTransitionPenalties", True)
    optimized.setdefault("preferSeparateSubgroupsByDay", True)
    return optimized


def _relaxed_cpsat_payload(payload):
    relaxed = deepcopy(payload)
    relaxed["enforceLectureBeforeLab"] = False
    relaxed["maxClassesPerDayForTeacher"] = max(8, int(relaxed.get("maxClassesPerDayForTeacher") or 0))
    relaxed["maxClassesPerDayForAudience"] = max(8, int(relaxed.get("maxClassesPerDayForAudience") or 0))
    relaxed["enableGapPenalties"] = False
    relaxed["enableBuildingTransitionPenalties"] = False
    relaxed["preferSeparateSubgroupsByDay"] = False
    relaxed["stopAfterFirstSolution"] = True
    relaxed["maxRoomCandidatesPerItem"] = max(
        int(relaxed.get("maxRoomCandidatesPerItem") or 0),
        CP_SAT_MAX_ROOM_CANDIDATES,
    )
    return relaxed


def optimize_cpsat_schedule(payload, context=None):
    context = context or {}
    primary_payload = _quality_cpsat_payload(payload)
    try:
        result = optimize_schedule(primary_payload)
        diagnostics = result.setdefault("diagnostics", {})
        diagnostics["relaxedRetryUsed"] = False
        diagnostics["cpsatMode"] = "quality-optimized"
        return result
    except ApiError as primary_exc:
        if primary_exc.code not in {"optimizer_no_solution", "optimizer_input_infeasible"}:
            raise

        relaxed_payload = _relaxed_cpsat_payload(primary_payload)
        try:
            result = optimize_schedule(relaxed_payload)
            diagnostics = result.setdefault("diagnostics", {})
            diagnostics["relaxedRetryUsed"] = True
            diagnostics["cpsatMode"] = "quality-relaxed-retry"
            diagnostics["primaryFailure"] = _batch_error_details(primary_exc, context, primary_payload)
            return result
        except ApiError as relaxed_exc:
            primary_details = _batch_error_details(primary_exc, context, primary_payload)
            relaxed_details = _batch_error_details(relaxed_exc, context, relaxed_payload)
            batch_key = context.get("batch_key") or {}
            study_course = context.get("study_course")
            raise ApiError(
                relaxed_exc.status,
                relaxed_exc.code,
                (
                    f"{relaxed_exc.message} CP-SAT primary and relaxed retry failed. "
                    f"Пакет: {study_course} курс, {batch_key.get('programme') or 'без направления'}, "
                    f"{batch_key.get('language') or 'без языка'}."
                ),
                details={
                    "primary": primary_details,
                    "relaxed": relaxed_details,
                },
            ) from relaxed_exc
