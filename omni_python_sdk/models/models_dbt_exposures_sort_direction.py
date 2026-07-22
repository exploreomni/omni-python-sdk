from typing import Literal

ModelsDbtExposuresSortDirection = Literal["asc", "desc"]

MODELS_DBT_EXPOSURES_SORT_DIRECTION_VALUES: set[ModelsDbtExposuresSortDirection] = {
    "asc",
    "desc",
}


def check_models_dbt_exposures_sort_direction(value: str) -> ModelsDbtExposuresSortDirection:
    if value in MODELS_DBT_EXPOSURES_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_DBT_EXPOSURES_SORT_DIRECTION_VALUES!r}")
