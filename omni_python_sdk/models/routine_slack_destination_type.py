from typing import Literal

RoutineSlackDestinationType = Literal["slack"]

ROUTINE_SLACK_DESTINATION_TYPE_VALUES: set[RoutineSlackDestinationType] = {
    "slack",
}


def check_routine_slack_destination_type(value: str) -> RoutineSlackDestinationType:
    if value in ROUTINE_SLACK_DESTINATION_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROUTINE_SLACK_DESTINATION_TYPE_VALUES!r}")
