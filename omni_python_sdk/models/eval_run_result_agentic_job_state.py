from typing import Literal

EvalRunResultAgenticJobState = Literal["CANCELLED", "COMPLETE", "DELIVERING", "EXECUTING", "FAILED", "QUEUED"]

EVAL_RUN_RESULT_AGENTIC_JOB_STATE_VALUES: set[EvalRunResultAgenticJobState] = {
    "CANCELLED",
    "COMPLETE",
    "DELIVERING",
    "EXECUTING",
    "FAILED",
    "QUEUED",
}


def check_eval_run_result_agentic_job_state(value: str) -> EvalRunResultAgenticJobState:
    if value in EVAL_RUN_RESULT_AGENTIC_JOB_STATE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {EVAL_RUN_RESULT_AGENTIC_JOB_STATE_VALUES!r}")
