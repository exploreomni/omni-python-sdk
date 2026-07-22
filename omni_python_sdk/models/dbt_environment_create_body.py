from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dbt_environment_variable import DbtEnvironmentVariable


T = TypeVar("T", bound="DbtEnvironmentCreateBody")


@_attrs_define
class DbtEnvironmentCreateBody:
    """
    Attributes:
        name (str): Environment name Example: PR_1111_Expose.
        target_schema (str): Target schema for this environment Example: PR_1111_Expose.
        is_deferral_enabled (bool | Unset): Whether to enable dbt deferral for this environment. Ignored (forced to
            false) for the default (production) environment. Default: False.
        owner_id (None | str | Unset): User ID of the environment owner. Used to mark development environments belonging
            to a specific user.
        target_database (None | str | Unset): Target database override Example: analytics_dev.
        target_name (None | str | Unset): Target name override
        target_role (None | str | Unset): Target role override
        variables (list[DbtEnvironmentVariable] | Unset): Environment variables
    """

    name: str
    target_schema: str
    is_deferral_enabled: bool | Unset = False
    owner_id: None | str | Unset = UNSET
    target_database: None | str | Unset = UNSET
    target_name: None | str | Unset = UNSET
    target_role: None | str | Unset = UNSET
    variables: list[DbtEnvironmentVariable] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        target_schema = self.target_schema

        is_deferral_enabled = self.is_deferral_enabled

        owner_id: None | str | Unset
        if isinstance(self.owner_id, Unset):
            owner_id = UNSET
        else:
            owner_id = self.owner_id

        target_database: None | str | Unset
        if isinstance(self.target_database, Unset):
            target_database = UNSET
        else:
            target_database = self.target_database

        target_name: None | str | Unset
        if isinstance(self.target_name, Unset):
            target_name = UNSET
        else:
            target_name = self.target_name

        target_role: None | str | Unset
        if isinstance(self.target_role, Unset):
            target_role = UNSET
        else:
            target_role = self.target_role

        variables: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.variables, Unset):
            variables = []
            for variables_item_data in self.variables:
                variables_item = variables_item_data.to_dict()
                variables.append(variables_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "targetSchema": target_schema,
            }
        )
        if is_deferral_enabled is not UNSET:
            field_dict["isDeferralEnabled"] = is_deferral_enabled
        if owner_id is not UNSET:
            field_dict["ownerId"] = owner_id
        if target_database is not UNSET:
            field_dict["targetDatabase"] = target_database
        if target_name is not UNSET:
            field_dict["targetName"] = target_name
        if target_role is not UNSET:
            field_dict["targetRole"] = target_role
        if variables is not UNSET:
            field_dict["variables"] = variables

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dbt_environment_variable import DbtEnvironmentVariable

        d = dict(src_dict)
        name = d.pop("name")

        target_schema = d.pop("targetSchema")

        is_deferral_enabled = d.pop("isDeferralEnabled", UNSET)

        def _parse_owner_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        owner_id = _parse_owner_id(d.pop("ownerId", UNSET))

        def _parse_target_database(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        target_database = _parse_target_database(d.pop("targetDatabase", UNSET))

        def _parse_target_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        target_name = _parse_target_name(d.pop("targetName", UNSET))

        def _parse_target_role(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        target_role = _parse_target_role(d.pop("targetRole", UNSET))

        _variables = d.pop("variables", UNSET)
        variables: list[DbtEnvironmentVariable] | Unset = UNSET
        if _variables is not UNSET:
            variables = []
            for variables_item_data in _variables:
                variables_item = DbtEnvironmentVariable.from_dict(variables_item_data)

                variables.append(variables_item)

        dbt_environment_create_body = cls(
            name=name,
            target_schema=target_schema,
            is_deferral_enabled=is_deferral_enabled,
            owner_id=owner_id,
            target_database=target_database,
            target_name=target_name,
            target_role=target_role,
            variables=variables,
        )

        dbt_environment_create_body.additional_properties = d
        return dbt_environment_create_body

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
