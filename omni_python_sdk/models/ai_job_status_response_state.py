from typing import Literal

AiJobStatusResponseState = Literal["CANCELLED", "COMPLETE", "DELIVERING", "EXECUTING", "FAILED", "QUEUED"]

AI_JOB_STATUS_RESPONSE_STATE_VALUES: set[AiJobStatusResponseState] = {
    "CANCELLED",
    "COMPLETE",
    "DELIVERING",
    "EXECUTING",
    "FAILED",
    "QUEUED",
}


def check_ai_job_status_response_state(value: str) -> AiJobStatusResponseState:
    if value in AI_JOB_STATUS_RESPONSE_STATE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_JOB_STATUS_RESPONSE_STATE_VALUES!r}")
