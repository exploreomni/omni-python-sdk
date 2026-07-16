from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="UsersAssignModelRoleResponse")


@_attrs_define
class UsersAssignModelRoleResponse:
    """
    Attributes:
        connection_id (UUID): The connection ID for this role assignment
        membership_id (UUID): The user membership ID
        model_id (UUID): The model ID for this role assignment
        role_name (str): The assigned role name Example: VIEWER.
    """

    connection_id: UUID
    membership_id: UUID
    model_id: UUID
    role_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_id = str(self.connection_id)

        membership_id = str(self.membership_id)

        model_id = str(self.model_id)

        role_name = self.role_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionId": connection_id,
                "membershipId": membership_id,
                "modelId": model_id,
                "roleName": role_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_id = UUID(d.pop("connectionId"))

        membership_id = UUID(d.pop("membershipId"))

        model_id = UUID(d.pop("modelId"))

        role_name = d.pop("roleName")

        users_assign_model_role_response = cls(
            connection_id=connection_id,
            membership_id=membership_id,
            model_id=model_id,
            role_name=role_name,
        )

        users_assign_model_role_response.additional_properties = d
        return users_assign_model_role_response

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
