from typing import Literal

ModelsRefreshResponseStatus = Literal["completed", "failed", "running"]

MODELS_REFRESH_RESPONSE_STATUS_VALUES: set[ModelsRefreshResponseStatus] = {
    "completed",
    "failed",
    "running",
}


def check_models_refresh_response_status(value: str) -> ModelsRefreshResponseStatus:
    if value in MODELS_REFRESH_RESPONSE_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_REFRESH_RESPONSE_STATUS_VALUES!r}")
