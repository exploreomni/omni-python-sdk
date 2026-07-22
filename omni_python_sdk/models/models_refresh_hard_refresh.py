from typing import Literal

ModelsRefreshHardRefresh = Literal["false", "true"]

MODELS_REFRESH_HARD_REFRESH_VALUES: set[ModelsRefreshHardRefresh] = {
    "false",
    "true",
}


def check_models_refresh_hard_refresh(value: str) -> ModelsRefreshHardRefresh:
    if value in MODELS_REFRESH_HARD_REFRESH_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_REFRESH_HARD_REFRESH_VALUES!r}")
