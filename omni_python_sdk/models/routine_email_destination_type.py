from typing import Literal

RoutineEmailDestinationType = Literal["email"]

ROUTINE_EMAIL_DESTINATION_TYPE_VALUES: set[RoutineEmailDestinationType] = {
    "email",
}


def check_routine_email_destination_type(value: str) -> RoutineEmailDestinationType:
    if value in ROUTINE_EMAIL_DESTINATION_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROUTINE_EMAIL_DESTINATION_TYPE_VALUES!r}")
