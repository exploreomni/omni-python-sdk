from typing import Literal

JobsGetStatusResponseStatus = Literal["COMPLETED", "FAILED", "IN_PROGRESS"]

JOBS_GET_STATUS_RESPONSE_STATUS_VALUES: set[JobsGetStatusResponseStatus] = {
    "COMPLETED",
    "FAILED",
    "IN_PROGRESS",
}


def check_jobs_get_status_response_status(value: str) -> JobsGetStatusResponseStatus:
    if value in JOBS_GET_STATUS_RESPONSE_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {JOBS_GET_STATUS_RESPONSE_STATUS_VALUES!r}")
