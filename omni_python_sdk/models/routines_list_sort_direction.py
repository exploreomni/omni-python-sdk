from typing import Literal

RoutinesListSortDirection = Literal["asc", "desc"]

ROUTINES_LIST_SORT_DIRECTION_VALUES: set[RoutinesListSortDirection] = {
    "asc",
    "desc",
}


def check_routines_list_sort_direction(value: str) -> RoutinesListSortDirection:
    if value in ROUTINES_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROUTINES_LIST_SORT_DIRECTION_VALUES!r}")
