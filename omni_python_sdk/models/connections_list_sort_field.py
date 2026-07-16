from typing import Literal

ConnectionsListSortField = Literal["database", "dialect", "name"]

CONNECTIONS_LIST_SORT_FIELD_VALUES: set[ConnectionsListSortField] = {
    "database",
    "dialect",
    "name",
}


def check_connections_list_sort_field(value: str) -> ConnectionsListSortField:
    if value in CONNECTIONS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONNECTIONS_LIST_SORT_FIELD_VALUES!r}")
