from typing import Literal

DbtExposureType = Literal["analysis", "application", "dashboard", "ml", "notebook"]

DBT_EXPOSURE_TYPE_VALUES: set[DbtExposureType] = {
    "analysis",
    "application",
    "dashboard",
    "ml",
    "notebook",
}


def check_dbt_exposure_type(value: str) -> DbtExposureType:
    if value in DBT_EXPOSURE_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DBT_EXPOSURE_TYPE_VALUES!r}")
