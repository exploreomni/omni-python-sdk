from typing import Literal

SchedulesCreateSchedulesCreateBodyDestinationType = Literal["email", "s3", "sftp", "slack", "webhook"]

SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_DESTINATION_TYPE_VALUES: set[
    SchedulesCreateSchedulesCreateBodyDestinationType
] = {
    "email",
    "s3",
    "sftp",
    "slack",
    "webhook",
}


def check_schedules_create_schedules_create_body_destination_type(
    value: str,
) -> SchedulesCreateSchedulesCreateBodyDestinationType:
    if value in SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_DESTINATION_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_DESTINATION_TYPE_VALUES!r}"
    )
