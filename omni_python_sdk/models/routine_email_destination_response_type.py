from typing import Literal

RoutineEmailDestinationResponseType = Literal["email"]

ROUTINE_EMAIL_DESTINATION_RESPONSE_TYPE_VALUES: set[RoutineEmailDestinationResponseType] = {
    "email",
}


def check_routine_email_destination_response_type(value: str) -> RoutineEmailDestinationResponseType:
    if value in ROUTINE_EMAIL_DESTINATION_RESPONSE_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROUTINE_EMAIL_DESTINATION_RESPONSE_TYPE_VALUES!r}")
