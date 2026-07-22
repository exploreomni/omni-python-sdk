from typing import Literal

SchedulesListStatus = Literal["canceled", "error", "none", "success"]

SCHEDULES_LIST_STATUS_VALUES: set[SchedulesListStatus] = {
    "canceled",
    "error",
    "none",
    "success",
}


def check_schedules_list_status(value: str) -> SchedulesListStatus:
    if value in SCHEDULES_LIST_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_STATUS_VALUES!r}")
