from typing import Literal

ConnectionsListConnectionsListResponseConnectionDialect = Literal[
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

CONNECTIONS_LIST_CONNECTIONS_LIST_RESPONSE_CONNECTION_DIALECT_VALUES: set[
    ConnectionsListConnectionsListResponseConnectionDialect
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


def check_connections_list_connections_list_response_connection_dialect(
    value: str,
) -> ConnectionsListConnectionsListResponseConnectionDialect:
    if value in CONNECTIONS_LIST_CONNECTIONS_LIST_RESPONSE_CONNECTION_DIALECT_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_LIST_CONNECTIONS_LIST_RESPONSE_CONNECTION_DIALECT_VALUES!r}"
    )
