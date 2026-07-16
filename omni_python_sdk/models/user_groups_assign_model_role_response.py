from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="UserGroupsAssignModelRoleResponse")


@_attrs_define
class UserGroupsAssignModelRoleResponse:
    """
    Attributes:
        connection_id (UUID): The connection ID for this role assignment
        model_id (UUID): The model ID for this role assignment
        role_name (str): The assigned role name Example: VIEWER.
        user_group_id (str): The user group short identifier Example: abc123.
    """

    connection_id: UUID
    model_id: UUID
    role_name: str
    user_group_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_id = str(self.connection_id)

        model_id = str(self.model_id)

        role_name = self.role_name

        user_group_id = self.user_group_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionId": connection_id,
                "modelId": model_id,
                "roleName": role_name,
                "userGroupId": user_group_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_id = UUID(d.pop("connectionId"))

        model_id = UUID(d.pop("modelId"))

        role_name = d.pop("roleName")

        user_group_id = d.pop("userGroupId")

        user_groups_assign_model_role_response = cls(
            connection_id=connection_id,
            model_id=model_id,
            role_name=role_name,
            user_group_id=user_group_id,
        )

        user_groups_assign_model_role_response.additional_properties = d
        return user_groups_assign_model_role_response

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
