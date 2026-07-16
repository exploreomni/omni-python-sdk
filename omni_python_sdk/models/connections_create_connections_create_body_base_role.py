from typing import Literal

ConnectionsCreateConnectionsCreateBodyBaseRole = Literal[
    "CONNECTION_ADMIN", "MODELER", "NO_ACCESS", "QUERIER", "RESTRICTED_QUERIER", "VIEWER"
]

CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_BASE_ROLE_VALUES: set[ConnectionsCreateConnectionsCreateBodyBaseRole] = {
    "CONNECTION_ADMIN",
    "MODELER",
    "NO_ACCESS",
    "QUERIER",
    "RESTRICTED_QUERIER",
    "VIEWER",
}


def check_connections_create_connections_create_body_base_role(
    value: str,
) -> ConnectionsCreateConnectionsCreateBodyBaseRole:
    if value in CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_BASE_ROLE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_BASE_ROLE_VALUES!r}"
    )
