from typing import Literal

EvalRunDetailStatus = Literal["CANCELLED", "COMPLETE", "RUNNING"]

EVAL_RUN_DETAIL_STATUS_VALUES: set[EvalRunDetailStatus] = {
    "CANCELLED",
    "COMPLETE",
    "RUNNING",
}


def check_eval_run_detail_status(value: str) -> EvalRunDetailStatus:
    if value in EVAL_RUN_DETAIL_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {EVAL_RUN_DETAIL_STATUS_VALUES!r}")
