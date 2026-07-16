from typing import Literal

SchedulesListSortField = Literal["dashboardName", "lastRun", "lastRunStatus", "ownerName", "scheduleName"]

SCHEDULES_LIST_SORT_FIELD_VALUES: set[SchedulesListSortField] = {
    "dashboardName",
    "lastRun",
    "lastRunStatus",
    "ownerName",
    "scheduleName",
}


def check_schedules_list_sort_field(value: str) -> SchedulesListSortField:
    if value in SCHEDULES_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_SORT_FIELD_VALUES!r}")
