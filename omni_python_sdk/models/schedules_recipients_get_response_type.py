from typing import Literal

SchedulesRecipientsGetResponseType = Literal["email", "google_sheets", "s3", "sftp", "slack", "webhook"]

SCHEDULES_RECIPIENTS_GET_RESPONSE_TYPE_VALUES: set[SchedulesRecipientsGetResponseType] = {
    "email",
    "google_sheets",
    "s3",
    "sftp",
    "slack",
    "webhook",
}


def check_schedules_recipients_get_response_type(value: str) -> SchedulesRecipientsGetResponseType:
    if value in SCHEDULES_RECIPIENTS_GET_RESPONSE_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULES_RECIPIENTS_GET_RESPONSE_TYPE_VALUES!r}")
