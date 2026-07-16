from typing import Literal

ConnectionsDbtEnvironmentsListSortField = Literal["name"]

CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_FIELD_VALUES: set[ConnectionsDbtEnvironmentsListSortField] = {
    "name",
}


def check_connections_dbt_environments_list_sort_field(value: str) -> ConnectionsDbtEnvironmentsListSortField:
    if value in CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_DBT_ENVIRONMENTS_LIST_SORT_FIELD_VALUES!r}"
    )
