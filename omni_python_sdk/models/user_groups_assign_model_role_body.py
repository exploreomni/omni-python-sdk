from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="UserGroupsAssignModelRoleBody")


@_attrs_define
class UserGroupsAssignModelRoleBody:
    """
    Attributes:
        role_name (str): Name of the role to assign (base or custom role) Example: VIEWER.
        connection_id (UUID | Unset): Connection ID for connection-level role assignment. Required if modelId not
            provided. Example: 550e8400-e29b-41d4-a716-446655440000.
        model_id (UUID | Unset): Model ID for model-level role assignment. Required if connectionId not provided.
            Example: 550e8400-e29b-41d4-a716-446655440000.
    """

    role_name: str
    connection_id: UUID | Unset = UNSET
    model_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role_name = self.role_name

        connection_id: str | Unset = UNSET
        if not isinstance(self.connection_id, Unset):
            connection_id = str(self.connection_id)

        model_id: str | Unset = UNSET
        if not isinstance(self.model_id, Unset):
            model_id = str(self.model_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "roleName": role_name,
            }
        )
        if connection_id is not UNSET:
            field_dict["connectionId"] = connection_id
        if model_id is not UNSET:
            field_dict["modelId"] = model_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        role_name = d.pop("roleName")

        _connection_id = d.pop("connectionId", UNSET)
        connection_id: UUID | Unset
        if isinstance(_connection_id, Unset):
            connection_id = UNSET
        else:
            connection_id = UUID(_connection_id)

        _model_id = d.pop("modelId", UNSET)
        model_id: UUID | Unset
        if isinstance(_model_id, Unset):
            model_id = UNSET
        else:
            model_id = UUID(_model_id)

        user_groups_assign_model_role_body = cls(
            role_name=role_name,
            connection_id=connection_id,
            model_id=model_id,
        )

        user_groups_assign_model_role_body.additional_properties = d
        return user_groups_assign_model_role_body

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
