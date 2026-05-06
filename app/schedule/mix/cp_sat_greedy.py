from ...core.errors import ApiError
from ..cp_sat import optimize_cpsat_schedule
from ..greedy import optimize_greedy_schedule


def optimize_cpsat_greedy_schedule(payload, context=None):
    try:
        return optimize_cpsat_schedule(payload, context=context)
    except ApiError as exc:
        if exc.code not in {"optimizer_no_solution", "optimizer_dependency_missing"}:
            raise

        result = optimize_greedy_schedule(payload)
        diagnostics = result.setdefault("diagnostics", {})
        diagnostics["hybridFallbackUsed"] = True
        diagnostics["cpsatFailure"] = getattr(exc, "details", None) or {}
        return result
