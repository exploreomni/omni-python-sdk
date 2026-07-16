from typing import Literal

AiJobCancelResponseState = Literal["CANCELLED", "COMPLETE", "DELIVERING", "EXECUTING", "FAILED", "QUEUED"]

AI_JOB_CANCEL_RESPONSE_STATE_VALUES: set[AiJobCancelResponseState] = {
    "CANCELLED",
    "COMPLETE",
    "DELIVERING",
    "EXECUTING",
    "FAILED",
    "QUEUED",
}


def check_ai_job_cancel_response_state(value: str) -> AiJobCancelResponseState:
    if value in AI_JOB_CANCEL_RESPONSE_STATE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_JOB_CANCEL_RESPONSE_STATE_VALUES!r}")
