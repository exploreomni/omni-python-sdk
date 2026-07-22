from typing import Literal

SchedulesListScheduleType = Literal["alert", "schedule"]

SCHEDULES_LIST_SCHEDULE_TYPE_VALUES: set[SchedulesListScheduleType] = {
    "alert",
    "schedule",
}


def check_schedules_list_schedule_type(value: str) -> SchedulesListScheduleType:
    if value in SCHEDULES_LIST_SCHEDULE_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_SCHEDULE_TYPE_VALUES!r}")
