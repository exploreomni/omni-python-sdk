from typing import Literal

ConnectionsCreateConnectionsCreateBodyDialect = Literal[
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "databricks_lakebase",
    "exasol",
    "mariadb",
    "motherduck",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "redshift",
    "sap_hana",
    "snowflake",
    "starrocks",
    "trino",
]

CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_DIALECT_VALUES: set[ConnectionsCreateConnectionsCreateBodyDialect] = {
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "databricks_lakebase",
    "exasol",
    "mariadb",
    "motherduck",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "redshift",
    "sap_hana",
    "snowflake",
    "starrocks",
    "trino",
}


def check_connections_create_connections_create_body_dialect(
    value: str,
) -> ConnectionsCreateConnectionsCreateBodyDialect:
    if value in CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_DIALECT_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONNECTIONS_CREATE_CONNECTIONS_CREATE_BODY_DIALECT_VALUES!r}"
    )
