from typing import Literal

ConnectionsDbtEnvironmentsListSortDirection = Literal["asc", "desc"]

CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_DIRECTION_VALUES: set[ConnectionsDbtEnvironmentsListSortDirection] = {
    "asc",
    "desc",
}


def check_connections_dbt_environments_list_sort_direction(value: str) -> ConnectionsDbtEnvironmentsListSortDirection:
    if value in CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_DIRECTION_VALUES!r}"
    )
