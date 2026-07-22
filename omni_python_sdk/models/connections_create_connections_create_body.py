from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connections_create_connections_create_body_base_role import (
    ConnectionsCreateConnectionsCreateBodyBaseRole,
    check_connections_create_connections_create_body_base_role,
)
from ..models.connections_create_connections_create_body_dialect import (
    ConnectionsCreateConnectionsCreateBodyDialect,
    check_connections_create_connections_create_body_dialect,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectionsCreateConnectionsCreateBody")


@_attrs_define
class ConnectionsCreateConnectionsCreateBody:
    """Request body for creating a database connection. Required fields: dialect, name, passwordUnencrypted. Additional
    fields may be required depending on the dialect.

        Attributes:
            dialect (ConnectionsCreateConnectionsCreateBodyDialect): The database dialect Example: snowflake.
            name (str): A descriptive name for the connection Example: Production Warehouse.
            password_unencrypted (str): The password to authenticate with. For BigQuery, this must be the JSON service
                account key file content. For Snowflake with keypair authentication, this can be omitted.
            accepts_license (bool | Unset): Acceptance of the license terms. Required for Oracle connections. Example: True.
            allows_user_specific_timezones (bool | Unset): Whether to allow users to specify their own timezones Default:
                False.
            always_scope_view_names (bool | Unset): Whether to always include schema (and catalog) prefixes in generated
                view names, even for tables in the default schema. Defaults to true for dialects that support multiple catalogs,
                false otherwise. Example: True.
            authentication_type (str | Unset): Authentication type. Applicable for BigQuery, MSSQL, Snowflake, Databricks,
                and Athena. Example: snowflake-password.
            aws_role_arn (str | Unset): AWS IAM role ARN. Applicable for Athena only. Example:
                arn:aws:iam::123456789012:role/OmniAthenaRole.
            base_role (ConnectionsCreateConnectionsCreateBodyBaseRole | Unset): The default role for users accessing the
                connection Example: QUERIER.
            database (str | Unset): The default database/catalog to connect to. For BigQuery, this is the project ID. For
                Athena, this is the data catalog. Example: analytics_db.
            default_schema (str | Unset): The default schema to use. Required for MSSQL. Example: public.
            enable_db_semantic_layer_integration (bool | Unset): Enable the dialect-native semantic layer integration.
                Applicable for Snowflake and Databricks. Default: False.
            enable_db_semantic_layer_topics (bool | Unset): Enable the dialect-native semantic layer topics. Applicable for
                Snowflake and Databricks. Default: False.
            external_oauth_audience (str | Unset): External OAuth audience claim. Applicable for Snowflake.
            external_oauth_authorization_url (str | Unset): External OAuth authorization URL (must be HTTPS). Applicable for
                Snowflake. Example: https://oauth.example.com/authorize.
            external_oauth_token_url (str | Unset): External OAuth token URL (must be HTTPS). Applicable for Snowflake.
                Example: https://oauth.example.com/token.
            host (str | Unset): The hostname or IP address of the database server. For Snowflake, provide only the account
                identifier. Example: myaccount.
            host_override (str | Unset): Custom Snowflake host (when not using the account identifier). Mutually exclusive
                with `host`. Example: myaccount.snowflakecomputing.com.
            include_other_catalogs (str | Unset): Comma-separated list of other catalogs/databases to include. Only
                applicable for databases that support multi-catalog queries. Example: other_project1,other_project2.
            include_schemas (str | Unset): Comma-separated list of schemas to include. Leave empty to include all schemas.
                Example: public,analytics.
            infer_relationships_from_column_names (bool | Unset): Whether to infer relationships from column-name
                conventions during schema refresh. Defaults to true. Default: True. Example: True.
            infer_relationships_from_foreign_keys (bool | Unset): Whether to infer relationships from declared foreign keys
                during schema refresh. Currently honored for Postgres and Snowflake. Default: False.
            max_billing_bytes (str | Unset): Maximum bytes that can be billed for a BigQuery query. Applicable for BigQuery
                only. Example: 1000000000.
            oauth_client_id (str | Unset): OAuth client ID for admin schema refresh. Applicable for Snowflake and
                Databricks.
            oauth_client_secret_unencrypted (str | Unset): OAuth client secret for admin schema refresh. Applicable for
                Snowflake and Databricks.
            offloaded_schemas (list[str] | str | Unset): Schemas whose tables should be queried via the offloaded engine.
                Accepts a comma-separated string or an array of schema names. Example: ['analytics_archive'].
            port (int | Unset): The port number for the database connection. Not required for Snowflake, MotherDuck,
                BigQuery, Databricks, and Athena. Example: 5432.
            private_key (str | Unset): An RSA key for keypair authentication. Omni will automatically add PEM headers if
                none are provided. Applicable for Snowflake only.
            query_timeout_seconds (int | Unset): The timeout in seconds for queries. Maximum value is 3600 (1 hour). Only
                applicable for databases that support query timeouts. Example: 900.
            query_timezone (str | Unset): The timezone to use for queries Example: NONE.
            region (str | Unset): Required for BigQuery and Athena connections. For BigQuery, specify a region like "us".
                For Athena, specify an AWS region like "us-east-1". Example: us-east-1.
            scratch_schema (str | Unset): Schema to use for data input (upload) tables. If not specified, a suitable default
                will be chosen. Example: omni_scratch.
            system_timezone (str | Unset): The timezone to use for the system Example: UTC.
            trust_server_certificate (bool | Unset): Whether to trust the server certificate. Applicable for MSSQL, Exasol,
                ClickHouse, Trino, and SAP HANA. Default: False.
            use_machine_auth (bool | Unset): Whether to authenticate using machine credentials (OAuth M2M). Applicable for
                Athena and Databricks.
            username (str | Unset): The username to authenticate with. For BigQuery, this is the client email from the
                service account. Example: analytics_user.
            warehouse (str | Unset): Required for Snowflake (specify the warehouse) and Databricks (specify the HTTP path).
                May be omitted for Snowflake OAuth connections, in which case each user's Snowflake default warehouse applies.
                Example: COMPUTE_WH.
            wif_audience (str | Unset): Full resource name of the workload identity pool provider. Required for BigQuery
                workload identity federation authentication. Example:
                //iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/my-pool/providers/my-provider.
            wif_service_account_email (str | Unset): Service account to impersonate for BigQuery workload identity
                federation authentication. When omitted, the federated identity is used directly. Example: omni@my-
                project.iam.gserviceaccount.com.
    """

    dialect: ConnectionsCreateConnectionsCreateBodyDialect
    name: str
    password_unencrypted: str
    accepts_license: bool | Unset = UNSET
    allows_user_specific_timezones: bool | Unset = False
    always_scope_view_names: bool | Unset = UNSET
    authentication_type: str | Unset = UNSET
    aws_role_arn: str | Unset = UNSET
    base_role: ConnectionsCreateConnectionsCreateBodyBaseRole | Unset = UNSET
    database: str | Unset = UNSET
    default_schema: str | Unset = UNSET
    enable_db_semantic_layer_integration: bool | Unset = False
    enable_db_semantic_layer_topics: bool | Unset = False
    external_oauth_audience: str | Unset = UNSET
    external_oauth_authorization_url: str | Unset = UNSET
    external_oauth_token_url: str | Unset = UNSET
    host: str | Unset = UNSET
    host_override: str | Unset = UNSET
    include_other_catalogs: str | Unset = UNSET
    include_schemas: str | Unset = UNSET
    infer_relationships_from_column_names: bool | Unset = True
    infer_relationships_from_foreign_keys: bool | Unset = False
    max_billing_bytes: str | Unset = UNSET
    oauth_client_id: str | Unset = UNSET
    oauth_client_secret_unencrypted: str | Unset = UNSET
    offloaded_schemas: list[str] | str | Unset = UNSET
    port: int | Unset = UNSET
    private_key: str | Unset = UNSET
    query_timeout_seconds: int | Unset = UNSET
    query_timezone: str | Unset = UNSET
    region: str | Unset = UNSET
    scratch_schema: str | Unset = UNSET
    system_timezone: str | Unset = UNSET
    trust_server_certificate: bool | Unset = False
    use_machine_auth: bool | Unset = UNSET
    username: str | Unset = UNSET
    warehouse: str | Unset = UNSET
    wif_audience: str | Unset = UNSET
    wif_service_account_email: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dialect: str = self.dialect

        name = self.name

        password_unencrypted = self.password_unencrypted

        accepts_license = self.accepts_license

        allows_user_specific_timezones = self.allows_user_specific_timezones

        always_scope_view_names = self.always_scope_view_names

        authentication_type = self.authentication_type

        aws_role_arn = self.aws_role_arn

        base_role: str | Unset = UNSET
        if not isinstance(self.base_role, Unset):
            base_role = self.base_role

        database = self.database

        default_schema = self.default_schema

        enable_db_semantic_layer_integration = self.enable_db_semantic_layer_integration

        enable_db_semantic_layer_topics = self.enable_db_semantic_layer_topics

        external_oauth_audience = self.external_oauth_audience

        external_oauth_authorization_url = self.external_oauth_authorization_url

        external_oauth_token_url = self.external_oauth_token_url

        host = self.host

        host_override = self.host_override

        include_other_catalogs = self.include_other_catalogs

        include_schemas = self.include_schemas

        infer_relationships_from_column_names = self.infer_relationships_from_column_names

        infer_relationships_from_foreign_keys = self.infer_relationships_from_foreign_keys

        max_billing_bytes = self.max_billing_bytes

        oauth_client_id = self.oauth_client_id

        oauth_client_secret_unencrypted = self.oauth_client_secret_unencrypted

        offloaded_schemas: list[str] | str | Unset
        if isinstance(self.offloaded_schemas, Unset):
            offloaded_schemas = UNSET
        elif isinstance(self.offloaded_schemas, list):
            offloaded_schemas = self.offloaded_schemas

        else:
            offloaded_schemas = self.offloaded_schemas

        port = self.port

        private_key = self.private_key

        query_timeout_seconds = self.query_timeout_seconds

        query_timezone = self.query_timezone

        region = self.region

        scratch_schema = self.scratch_schema

        system_timezone = self.system_timezone

        trust_server_certificate = self.trust_server_certificate

        use_machine_auth = self.use_machine_auth

        username = self.username

        warehouse = self.warehouse

        wif_audience = self.wif_audience

        wif_service_account_email = self.wif_service_account_email

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "dialect": dialect,
                "name": name,
                "passwordUnencrypted": password_unencrypted,
            }
        )
        if accepts_license is not UNSET:
            field_dict["acceptsLicense"] = accepts_license
        if allows_user_specific_timezones is not UNSET:
            field_dict["allowsUserSpecificTimezones"] = allows_user_specific_timezones
        if always_scope_view_names is not UNSET:
            field_dict["alwaysScopeViewNames"] = always_scope_view_names
        if authentication_type is not UNSET:
            field_dict["authenticationType"] = authentication_type
        if aws_role_arn is not UNSET:
            field_dict["awsRoleArn"] = aws_role_arn
        if base_role is not UNSET:
            field_dict["baseRole"] = base_role
        if database is not UNSET:
            field_dict["database"] = database
        if default_schema is not UNSET:
            field_dict["defaultSchema"] = default_schema
        if enable_db_semantic_layer_integration is not UNSET:
            field_dict["enableDbSemanticLayerIntegration"] = enable_db_semantic_layer_integration
        if enable_db_semantic_layer_topics is not UNSET:
            field_dict["enableDbSemanticLayerTopics"] = enable_db_semantic_layer_topics
        if external_oauth_audience is not UNSET:
            field_dict["externalOauthAudience"] = external_oauth_audience
        if external_oauth_authorization_url is not UNSET:
            field_dict["externalOauthAuthorizationUrl"] = external_oauth_authorization_url
        if external_oauth_token_url is not UNSET:
            field_dict["externalOauthTokenUrl"] = external_oauth_token_url
        if host is not UNSET:
            field_dict["host"] = host
        if host_override is not UNSET:
            field_dict["hostOverride"] = host_override
        if include_other_catalogs is not UNSET:
            field_dict["includeOtherCatalogs"] = include_other_catalogs
        if include_schemas is not UNSET:
            field_dict["includeSchemas"] = include_schemas
        if infer_relationships_from_column_names is not UNSET:
            field_dict["inferRelationshipsFromColumnNames"] = infer_relationships_from_column_names
        if infer_relationships_from_foreign_keys is not UNSET:
            field_dict["inferRelationshipsFromForeignKeys"] = infer_relationships_from_foreign_keys
        if max_billing_bytes is not UNSET:
            field_dict["maxBillingBytes"] = max_billing_bytes
        if oauth_client_id is not UNSET:
            field_dict["oauthClientId"] = oauth_client_id
        if oauth_client_secret_unencrypted is not UNSET:
            field_dict["oauthClientSecretUnencrypted"] = oauth_client_secret_unencrypted
        if offloaded_schemas is not UNSET:
            field_dict["offloadedSchemas"] = offloaded_schemas
        if port is not UNSET:
            field_dict["port"] = port
        if private_key is not UNSET:
            field_dict["privateKey"] = private_key
        if query_timeout_seconds is not UNSET:
            field_dict["queryTimeoutSeconds"] = query_timeout_seconds
        if query_timezone is not UNSET:
            field_dict["queryTimezone"] = query_timezone
        if region is not UNSET:
            field_dict["region"] = region
        if scratch_schema is not UNSET:
            field_dict["scratchSchema"] = scratch_schema
        if system_timezone is not UNSET:
            field_dict["systemTimezone"] = system_timezone
        if trust_server_certificate is not UNSET:
            field_dict["trustServerCertificate"] = trust_server_certificate
        if use_machine_auth is not UNSET:
            field_dict["useMachineAuth"] = use_machine_auth
        if username is not UNSET:
            field_dict["username"] = username
        if warehouse is not UNSET:
            field_dict["warehouse"] = warehouse
        if wif_audience is not UNSET:
            field_dict["wifAudience"] = wif_audience
        if wif_service_account_email is not UNSET:
            field_dict["wifServiceAccountEmail"] = wif_service_account_email

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dialect = check_connections_create_connections_create_body_dialect(d.pop("dialect"))

        name = d.pop("name")

        password_unencrypted = d.pop("passwordUnencrypted")

        accepts_license = d.pop("acceptsLicense", UNSET)

        allows_user_specific_timezones = d.pop("allowsUserSpecificTimezones", UNSET)

        always_scope_view_names = d.pop("alwaysScopeViewNames", UNSET)

        authentication_type = d.pop("authenticationType", UNSET)

        aws_role_arn = d.pop("awsRoleArn", UNSET)

        _base_role = d.pop("baseRole", UNSET)
        base_role: ConnectionsCreateConnectionsCreateBodyBaseRole | Unset
        if isinstance(_base_role, Unset):
            base_role = UNSET
        else:
            base_role = check_connections_create_connections_create_body_base_role(_base_role)

        database = d.pop("database", UNSET)

        default_schema = d.pop("defaultSchema", UNSET)

        enable_db_semantic_layer_integration = d.pop("enableDbSemanticLayerIntegration", UNSET)

        enable_db_semantic_layer_topics = d.pop("enableDbSemanticLayerTopics", UNSET)

        external_oauth_audience = d.pop("externalOauthAudience", UNSET)

        external_oauth_authorization_url = d.pop("externalOauthAuthorizationUrl", UNSET)

        external_oauth_token_url = d.pop("externalOauthTokenUrl", UNSET)

        host = d.pop("host", UNSET)

        host_override = d.pop("hostOverride", UNSET)

        include_other_catalogs = d.pop("includeOtherCatalogs", UNSET)

        include_schemas = d.pop("includeSchemas", UNSET)

        infer_relationships_from_column_names = d.pop("inferRelationshipsFromColumnNames", UNSET)

        infer_relationships_from_foreign_keys = d.pop("inferRelationshipsFromForeignKeys", UNSET)

        max_billing_bytes = d.pop("maxBillingBytes", UNSET)

        oauth_client_id = d.pop("oauthClientId", UNSET)

        oauth_client_secret_unencrypted = d.pop("oauthClientSecretUnencrypted", UNSET)

        def _parse_offloaded_schemas(data: object) -> list[str] | str | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                offloaded_schemas_type_1 = cast(list[str], data)

                return offloaded_schemas_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | str | Unset, data)

        offloaded_schemas = _parse_offloaded_schemas(d.pop("offloadedSchemas", UNSET))

        port = d.pop("port", UNSET)

        private_key = d.pop("privateKey", UNSET)

        query_timeout_seconds = d.pop("queryTimeoutSeconds", UNSET)

        query_timezone = d.pop("queryTimezone", UNSET)

        region = d.pop("region", UNSET)

        scratch_schema = d.pop("scratchSchema", UNSET)

        system_timezone = d.pop("systemTimezone", UNSET)

        trust_server_certificate = d.pop("trustServerCertificate", UNSET)

        use_machine_auth = d.pop("useMachineAuth", UNSET)

        username = d.pop("username", UNSET)

        warehouse = d.pop("warehouse", UNSET)

        wif_audience = d.pop("wifAudience", UNSET)

        wif_service_account_email = d.pop("wifServiceAccountEmail", UNSET)

        connections_create_connections_create_body = cls(
            dialect=dialect,
            name=name,
            password_unencrypted=password_unencrypted,
            accepts_license=accepts_license,
            allows_user_specific_timezones=allows_user_specific_timezones,
            always_scope_view_names=always_scope_view_names,
            authentication_type=authentication_type,
            aws_role_arn=aws_role_arn,
            base_role=base_role,
            database=database,
            default_schema=default_schema,
            enable_db_semantic_layer_integration=enable_db_semantic_layer_integration,
            enable_db_semantic_layer_topics=enable_db_semantic_layer_topics,
            external_oauth_audience=external_oauth_audience,
            external_oauth_authorization_url=external_oauth_authorization_url,
            external_oauth_token_url=external_oauth_token_url,
            host=host,
            host_override=host_override,
            include_other_catalogs=include_other_catalogs,
            include_schemas=include_schemas,
            infer_relationships_from_column_names=infer_relationships_from_column_names,
            infer_relationships_from_foreign_keys=infer_relationships_from_foreign_keys,
            max_billing_bytes=max_billing_bytes,
            oauth_client_id=oauth_client_id,
            oauth_client_secret_unencrypted=oauth_client_secret_unencrypted,
            offloaded_schemas=offloaded_schemas,
            port=port,
            private_key=private_key,
            query_timeout_seconds=query_timeout_seconds,
            query_timezone=query_timezone,
            region=region,
            scratch_schema=scratch_schema,
            system_timezone=system_timezone,
            trust_server_certificate=trust_server_certificate,
            use_machine_auth=use_machine_auth,
            username=username,
            warehouse=warehouse,
            wif_audience=wif_audience,
            wif_service_account_email=wif_service_account_email,
        )

        connections_create_connections_create_body.additional_properties = d
        return connections_create_connections_create_body

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
