from typing import Literal

SchedulesListContentType = Literal["dashboard", "single tile"]

SCHEDULES_LIST_CONTENT_TYPE_VALUES: set[SchedulesListContentType] = {
    "dashboard",
    "single tile",
}


def check_schedules_list_content_type(value: str) -> SchedulesListContentType:
    if value in SCHEDULES_LIST_CONTENT_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_CONTENT_TYPE_VALUES!r}")
