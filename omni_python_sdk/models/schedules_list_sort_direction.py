from typing import Literal

SchedulesListSortDirection = Literal["asc", "desc"]

SCHEDULES_LIST_SORT_DIRECTION_VALUES: set[SchedulesListSortDirection] = {
    "asc",
    "desc",
}


def check_schedules_list_sort_direction(value: str) -> SchedulesListSortDirection:
    if value in SCHEDULES_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_SORT_DIRECTION_VALUES!r}")
