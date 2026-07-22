from typing import Literal

ConnectionsGetConnectionsGetResponseConnectionDialect = Literal[
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "databricks_lakebase",
    "duckdb",
    "mariadb",
    "motherduck",
    "mysql",
    "postgres",
    "redshift",
    "singlestore",
    "snowflake",
    "sqlserver",
    "trino",
]

CONNECTIONS_GET_CONNECTIONS_GET_RESPONSE_CONNECTION_DIALECT_VALUES: set[
    ConnectionsGetConnectionsGetResponseConnectionDialect
] = {
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "databricks_lakebase",
    "duckdb",
    "mariadb",
    "motherduck",
    "mysql",
    "postgres",
    "redshift",
    "singlestore",
    "snowflake",
    "sqlserver",
    "trino",
}


def check_connections_get_connections_get_response_connection_dialect(
    value: str,
) -> ConnectionsGetConnectionsGetResponseConnectionDialect:
    if value in CONNECTIONS_GET_CONNECTIONS_GET_RESPONSE_CONNECTION_DIALECT_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_GET_CONNECTIONS_GET_RESPONSE_CONNECTION_DIALECT_VALUES!r}"
    )
