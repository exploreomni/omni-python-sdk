from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.dbt_environment_response_variable import DbtEnvironmentResponseVariable


T = TypeVar("T", bound="DbtEnvironmentItem")


@_attrs_define
class DbtEnvironmentItem:
    """
    Attributes:
        id (UUID): Unique environment identifier
        is_default_environment (bool): Whether this is the default environment
        is_deferral_enabled (bool): Whether dbt deferral is enabled for this environment. Always false for the default
            (production) environment — the backend rejects enabling it there.
        name (str): Environment name
        owner_id (None | str): User ID of the environment owner, or null if not a personal environment
        target_database (None | str): Target database override
        target_name (None | str): Target name override
        target_role (None | str): Target role override
        target_schema (str): Target schema
        variables (list[DbtEnvironmentResponseVariable]): Environment variables
    """

    id: UUID
    is_default_environment: bool
    is_deferral_enabled: bool
    name: str
    owner_id: None | str
    target_database: None | str
    target_name: None | str
    target_role: None | str
    target_schema: str
    variables: list[DbtEnvironmentResponseVariable]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        is_default_environment = self.is_default_environment

        is_deferral_enabled = self.is_deferral_enabled

        name = self.name

        owner_id: None | str
        owner_id = self.owner_id

        target_database: None | str
        target_database = self.target_database

        target_name: None | str
        target_name = self.target_name

        target_role: None | str
        target_role = self.target_role

        target_schema = self.target_schema

        variables = []
        for variables_item_data in self.variables:
            variables_item = variables_item_data.to_dict()
            variables.append(variables_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "isDefaultEnvironment": is_default_environment,
                "isDeferralEnabled": is_deferral_enabled,
                "name": name,
                "ownerId": owner_id,
                "targetDatabase": target_database,
                "targetName": target_name,
                "targetRole": target_role,
                "targetSchema": target_schema,
                "variables": variables,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dbt_environment_response_variable import DbtEnvironmentResponseVariable

        d = dict(src_dict)
        id = UUID(d.pop("id"))

        is_default_environment = d.pop("isDefaultEnvironment")

        is_deferral_enabled = d.pop("isDeferralEnabled")

        name = d.pop("name")

        def _parse_owner_id(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        owner_id = _parse_owner_id(d.pop("ownerId"))

        def _parse_target_database(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        target_database = _parse_target_database(d.pop("targetDatabase"))

        def _parse_target_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        target_name = _parse_target_name(d.pop("targetName"))

        def _parse_target_role(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        target_role = _parse_target_role(d.pop("targetRole"))

        target_schema = d.pop("targetSchema")

        variables = []
        _variables = d.pop("variables")
        for variables_item_data in _variables:
            variables_item = DbtEnvironmentResponseVariable.from_dict(variables_item_data)

            variables.append(variables_item)

        dbt_environment_item = cls(
            id=id,
            is_default_environment=is_default_environment,
            is_deferral_enabled=is_deferral_enabled,
            name=name,
            owner_id=owner_id,
            target_database=target_database,
            target_name=target_name,
            target_role=target_role,
            target_schema=target_schema,
            variables=variables,
        )

        dbt_environment_item.additional_properties = d
        return dbt_environment_item

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
