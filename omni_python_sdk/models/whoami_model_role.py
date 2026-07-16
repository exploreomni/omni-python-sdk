from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.whoami_model_role_permissions_item import (
    WhoamiModelRolePermissionsItem,
    check_whoami_model_role_permissions_item,
)

T = TypeVar("T", bound="WhoamiModelRole")


@_attrs_define
class WhoamiModelRole:
    """
    Attributes:
        base_role (str): The resolved base role (for custom roles, the base role they extend). Example: QUERIER.
        connection_id (str): The connection this model belongs to
        permissions (list[WhoamiModelRolePermissionsItem]): The caller's resolved/effective permissions on this model,
            reflecting custom roles. This is a capability signal for the directly-roleable model kinds (schema / shared /
            extension). It does not enumerate the permissions you derive on branch, workbook, and query models from your
            role on the base model they descend from — absence here does not mean you lack access on those derived models.
            MANAGE_MODEL, READ, and REFRESH_SCHEMA are also not reported: they derive from connection / sibling-model roles
            rather than a per-model rule. Example: ['QUERY_TOPICS', 'QUERY_SQL', 'USE_WORKBOOKS'].
        role_name (str): The resolved role name (informational; may be a custom role). Use `permissions` to decide
            capability. Example: QUERIER.
    """

    base_role: str
    connection_id: str
    permissions: list[WhoamiModelRolePermissionsItem]
    role_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_role = self.base_role

        connection_id = self.connection_id

        permissions = []
        for permissions_item_data in self.permissions:
            permissions_item: str = permissions_item_data
            permissions.append(permissions_item)

        role_name = self.role_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseRole": base_role,
                "connectionId": connection_id,
                "permissions": permissions,
                "roleName": role_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        base_role = d.pop("baseRole")

        connection_id = d.pop("connectionId")

        permissions = []
        _permissions = d.pop("permissions")
        for permissions_item_data in _permissions:
            permissions_item = check_whoami_model_role_permissions_item(permissions_item_data)

            permissions.append(permissions_item)

        role_name = d.pop("roleName")

        whoami_model_role = cls(
            base_role=base_role,
            connection_id=connection_id,
            permissions=permissions,
            role_name=role_name,
        )

        whoami_model_role.additional_properties = d
        return whoami_model_role

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
