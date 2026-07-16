from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connections_get_connections_get_response_connection_dialect import (
    ConnectionsGetConnectionsGetResponseConnectionDialect,
    check_connections_get_connections_get_response_connection_dialect,
)

T = TypeVar("T", bound="ConnectionsGetConnectionsGetResponseConnection")


@_attrs_define
class ConnectionsGetConnectionsGetResponseConnection:
    """Connection object

    Attributes:
        allow_branch_connection_environments (bool | None): Whether a branch may select its own connection environment.
            When user-attribute environment selection is also enabled, a branch selection overrides the user attribute.
        base_role (None | str): Default role for users on this connection Example: QUERIER.
        branch_connection_environment_overrides_user_attr (bool | None): Deprecated alias for
            `allowBranchConnectionEnvironments`; same value. Use `allowBranchConnectionEnvironments` instead.
        created_at (str): Timestamp when connection was created (ISO 8601) Example: 2024-01-15T10:30:00Z.
        database (None | str): Database name Example: analytics_db.
        default_schema (None | str): Default schema for the connection Example: public.
        deleted_at (None | str): Timestamp when connection was deleted (ISO 8601)
        dialect (ConnectionsGetConnectionsGetResponseConnectionDialect): Database dialect type Example: snowflake.
        environment_connection_switches_schema_model (bool | None): Whether environment connections switch schema model
        id (UUID): Unique connection identifier Example: 550e8400-e29b-41d4-a716-446655440000.
        name (str): Connection display name Example: Production Snowflake.
        updated_at (str): Timestamp when connection was last updated (ISO 8601) Example: 2024-01-15T10:30:00Z.
        user_attribute_name_for_connection_environments (None | str): User attribute name used for connection
            environments Example: region.
        user_attribute_values_for_default_environment (list[str] | None): Default user attribute values for the base
            environment Example: ['us-east', 'us-west'].
    """

    allow_branch_connection_environments: bool | None
    base_role: None | str
    branch_connection_environment_overrides_user_attr: bool | None
    created_at: str
    database: None | str
    default_schema: None | str
    deleted_at: None | str
    dialect: ConnectionsGetConnectionsGetResponseConnectionDialect
    environment_connection_switches_schema_model: bool | None
    id: UUID
    name: str
    updated_at: str
    user_attribute_name_for_connection_environments: None | str
    user_attribute_values_for_default_environment: list[str] | None
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        allow_branch_connection_environments: bool | None
        allow_branch_connection_environments = self.allow_branch_connection_environments

        base_role: None | str
        base_role = self.base_role

        branch_connection_environment_overrides_user_attr: bool | None
        branch_connection_environment_overrides_user_attr = self.branch_connection_environment_overrides_user_attr

        created_at = self.created_at

        database: None | str
        database = self.database

        default_schema: None | str
        default_schema = self.default_schema

        deleted_at: None | str
        deleted_at = self.deleted_at

        dialect: str = self.dialect

        environment_connection_switches_schema_model: bool | None
        environment_connection_switches_schema_model = self.environment_connection_switches_schema_model

        id = str(self.id)

        name = self.name

        updated_at = self.updated_at

        user_attribute_name_for_connection_environments: None | str
        user_attribute_name_for_connection_environments = self.user_attribute_name_for_connection_environments

        user_attribute_values_for_default_environment: list[str] | None
        if isinstance(self.user_attribute_values_for_default_environment, list):
            user_attribute_values_for_default_environment = self.user_attribute_values_for_default_environment

        else:
            user_attribute_values_for_default_environment = self.user_attribute_values_for_default_environment

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "allowBranchConnectionEnvironments": allow_branch_connection_environments,
                "baseRole": base_role,
                "branchConnectionEnvironmentOverridesUserAttr": branch_connection_environment_overrides_user_attr,
                "createdAt": created_at,
                "database": database,
                "defaultSchema": default_schema,
                "deletedAt": deleted_at,
                "dialect": dialect,
                "environmentConnectionSwitchesSchemaModel": environment_connection_switches_schema_model,
                "id": id,
                "name": name,
                "updatedAt": updated_at,
                "userAttributeNameForConnectionEnvironments": user_attribute_name_for_connection_environments,
                "userAttributeValuesForDefaultEnvironment": user_attribute_values_for_default_environment,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_allow_branch_connection_environments(data: object) -> bool | None:
            if data is None:
                return data
            return cast(bool | None, data)

        allow_branch_connection_environments = _parse_allow_branch_connection_environments(
            d.pop("allowBranchConnectionEnvironments")
        )

        def _parse_base_role(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        base_role = _parse_base_role(d.pop("baseRole"))

        def _parse_branch_connection_environment_overrides_user_attr(data: object) -> bool | None:
            if data is None:
                return data
            return cast(bool | None, data)

        branch_connection_environment_overrides_user_attr = _parse_branch_connection_environment_overrides_user_attr(
            d.pop("branchConnectionEnvironmentOverridesUserAttr")
        )

        created_at = d.pop("createdAt")

        def _parse_database(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        database = _parse_database(d.pop("database"))

        def _parse_default_schema(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        default_schema = _parse_default_schema(d.pop("defaultSchema"))

        def _parse_deleted_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        deleted_at = _parse_deleted_at(d.pop("deletedAt"))

        dialect = check_connections_get_connections_get_response_connection_dialect(d.pop("dialect"))

        def _parse_environment_connection_switches_schema_model(data: object) -> bool | None:
            if data is None:
                return data
            return cast(bool | None, data)

        environment_connection_switches_schema_model = _parse_environment_connection_switches_schema_model(
            d.pop("environmentConnectionSwitchesSchemaModel")
        )

        id = UUID(d.pop("id"))

        name = d.pop("name")

        updated_at = d.pop("updatedAt")

        def _parse_user_attribute_name_for_connection_environments(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        user_attribute_name_for_connection_environments = _parse_user_attribute_name_for_connection_environments(
            d.pop("userAttributeNameForConnectionEnvironments")
        )

        def _parse_user_attribute_values_for_default_environment(data: object) -> list[str] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                user_attribute_values_for_default_environment_type_0 = cast(list[str], data)

                return user_attribute_values_for_default_environment_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None, data)

        user_attribute_values_for_default_environment = _parse_user_attribute_values_for_default_environment(
            d.pop("userAttributeValuesForDefaultEnvironment")
        )

        connections_get_connections_get_response_connection = cls(
            allow_branch_connection_environments=allow_branch_connection_environments,
            base_role=base_role,
            branch_connection_environment_overrides_user_attr=branch_connection_environment_overrides_user_attr,
            created_at=created_at,
            database=database,
            default_schema=default_schema,
            deleted_at=deleted_at,
            dialect=dialect,
            environment_connection_switches_schema_model=environment_connection_switches_schema_model,
            id=id,
            name=name,
            updated_at=updated_at,
            user_attribute_name_for_connection_environments=user_attribute_name_for_connection_environments,
            user_attribute_values_for_default_environment=user_attribute_values_for_default_environment,
        )

        connections_get_connections_get_response_connection.additional_properties = d
        return connections_get_connections_get_response_connection

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
