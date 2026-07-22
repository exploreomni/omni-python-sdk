from typing import Literal

SchedulesCreateSchedulesCreateBodyConditionType = Literal[
    "RESULTS_CHANGED", "RESULTS_MISSING", "RESULTS_PRESENT", "RESULTS_UNCHANGED"
]

SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_CONDITION_TYPE_VALUES: set[SchedulesCreateSchedulesCreateBodyConditionType] = {
    "RESULTS_CHANGED",
    "RESULTS_MISSING",
    "RESULTS_PRESENT",
    "RESULTS_UNCHANGED",
}


def check_schedules_create_schedules_create_body_condition_type(
    value: str,
) -> SchedulesCreateSchedulesCreateBodyConditionType:
    if value in SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_CONDITION_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCHEDULES_CREATE_SCHEDULES_CREATE_BODY_CONDITION_TYPE_VALUES!r}"
    )
