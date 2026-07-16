from typing import Literal

SchedulesListDestination = Literal["email", "google_sheets", "s3", "sftp", "slack", "webhook"]

SCHEDULES_LIST_DESTINATION_VALUES: set[SchedulesListDestination] = {
    "email",
    "google_sheets",
    "s3",
    "sftp",
    "slack",
    "webhook",
}


def check_schedules_list_destination(value: str) -> SchedulesListDestination:
    if value in SCHEDULES_LIST_DESTINATION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_LIST_DESTINATION_VALUES!r}")
