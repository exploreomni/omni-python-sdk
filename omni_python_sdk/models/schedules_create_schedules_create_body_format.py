from typing import Literal

SchedulesCreateSchedulesCreateBodyFormat = Literal["csv", "json", "link_only", "pdf", "png", "xlsx"]

SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_FORMAT_VALUES: set[SchedulesCreateSchedulesCreateBodyFormat] = {
    "csv",
    "json",
    "link_only",
    "pdf",
    "png",
    "xlsx",
}


def check_schedules_create_schedules_create_body_format(value: str) -> SchedulesCreateSchedulesCreateBodyFormat:
    if value in SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_FORMAT_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_FORMAT_VALUES!r}"
    )
