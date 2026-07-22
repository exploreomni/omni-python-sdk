from typing import Literal

ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1 = Literal[""]

CONNECTIONS_DBT_UPDATE_CONNECTIONS_DBT_UPDATE_BODY_PROJECT_ROOT_PATH_TYPE_1_VALUES: set[
    ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1
] = {
    "",
}


def check_connections_dbt_update_connections_dbt_update_body_project_root_path_type_1(
    value: str,
) -> ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1:
    if value in CONNECTIONS_DBT_UPDATE_CONNECTIONS_DBT_UPDATE_BODY_PROJECT_ROOT_PATH_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_DBT_UPDATE_CONNECTIONS_DBT_UPDATE_BODY_PROJECT_ROOT_PATH_TYPE_1_VALUES!r}"
    )
