from typing import Literal

RoutineSlackDestinationSlackRecipientType = Literal["channel", "users"]

ROUTINE_SLACK_DESTINATION_SLACK_RECIPIENT_TYPE_VALUES: set[RoutineSlackDestinationSlackRecipientType] = {
    "channel",
    "users",
}


def check_routine_slack_destination_slack_recipient_type(value: str) -> RoutineSlackDestinationSlackRecipientType:
    if value in ROUTINE_SLACK_DESTINATION_SLACK_RECIPIENT_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {ROUTINE_SLACK_DESTINATION_SLACK_RECIPIENT_TYPE_VALUES!r}"
    )
