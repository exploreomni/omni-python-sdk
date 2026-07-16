from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FoldersGetPermissionsResponsePermitsItem")


@_attrs_define
class FoldersGetPermissionsResponsePermitsItem:
    """
    Attributes:
        role (str): Content role (e.g., VIEWER, EDITOR, MANAGER) Example: VIEWER.
        access_boost (bool | Unset): Whether access boost is enabled for this permit
        user_group_id (str | Unset): User group ID if this is a group permit
        user_id (UUID | Unset): User ID if this is a user permit
    """

    role: str
    access_boost: bool | Unset = UNSET
    user_group_id: str | Unset = UNSET
    user_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role = self.role

        access_boost = self.access_boost

        user_group_id = self.user_group_id

        user_id: str | Unset = UNSET
        if not isinstance(self.user_id, Unset):
            user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "role": role,
            }
        )
        if access_boost is not UNSET:
            field_dict["accessBoost"] = access_boost
        if user_group_id is not UNSET:
            field_dict["userGroupId"] = user_group_id
        if user_id is not UNSET:
            field_dict["userId"] = user_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        role = d.pop("role")

        access_boost = d.pop("accessBoost", UNSET)

        user_group_id = d.pop("userGroupId", UNSET)

        _user_id = d.pop("userId", UNSET)
        user_id: UUID | Unset
        if isinstance(_user_id, Unset):
            user_id = UNSET
        else:
            user_id = UUID(_user_id)

        folders_get_permissions_response_permits_item = cls(
            role=role,
            access_boost=access_boost,
            user_group_id=user_group_id,
            user_id=user_id,
        )

        folders_get_permissions_response_permits_item.additional_properties = d
        return folders_get_permissions_response_permits_item

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
