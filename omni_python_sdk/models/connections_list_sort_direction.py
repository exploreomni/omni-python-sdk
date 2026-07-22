from typing import Literal

ConnectionsListSortDirection = Literal["asc", "desc"]

CONNECTIONS_LIST_SORT_DIRECTION_VALUES: set[ConnectionsListSortDirection] = {
    "asc",
    "desc",
}


def check_connections_list_sort_direction(value: str) -> ConnectionsListSortDirection:
    if value in CONNECTIONS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONNECTIONS_LIST_SORT_DIRECTION_VALUES!r}")
