from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..models.folders_update_permissions_body_role import (
    FoldersUpdatePermissionsBodyRole,
    check_folders_update_permissions_body_role,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="FoldersUpdatePermissionsBody")


@_attrs_define
class FoldersUpdatePermissionsBody:
    """
    Attributes:
        access_boost (bool | Unset): Whether to grant access boost
        role (FoldersUpdatePermissionsBodyRole | Unset): New content role to assign
        user_group_ids (list[str] | Unset): User group IDs to update permissions for
        user_ids (list[UUID] | Unset): User IDs to update permissions for
    """

    access_boost: bool | Unset = UNSET
    role: FoldersUpdatePermissionsBodyRole | Unset = UNSET
    user_group_ids: list[str] | Unset = UNSET
    user_ids: list[UUID] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        access_boost = self.access_boost

        role: str | Unset = UNSET
        if not isinstance(self.role, Unset):
            role = self.role

        user_group_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_group_ids, Unset):
            user_group_ids = self.user_group_ids

        user_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_ids, Unset):
            user_ids = []
            for user_ids_item_data in self.user_ids:
                user_ids_item = str(user_ids_item_data)
                user_ids.append(user_ids_item)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if access_boost is not UNSET:
            field_dict["accessBoost"] = access_boost
        if role is not UNSET:
            field_dict["role"] = role
        if user_group_ids is not UNSET:
            field_dict["userGroupIds"] = user_group_ids
        if user_ids is not UNSET:
            field_dict["userIds"] = user_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        access_boost = d.pop("accessBoost", UNSET)

        _role = d.pop("role", UNSET)
        role: FoldersUpdatePermissionsBodyRole | Unset
        if isinstance(_role, Unset):
            role = UNSET
        else:
            role = check_folders_update_permissions_body_role(_role)

        user_group_ids = cast(list[str], d.pop("userGroupIds", UNSET))

        _user_ids = d.pop("userIds", UNSET)
        user_ids: list[UUID] | Unset = UNSET
        if _user_ids is not UNSET:
            user_ids = []
            for user_ids_item_data in _user_ids:
                user_ids_item = UUID(user_ids_item_data)

                user_ids.append(user_ids_item)

        folders_update_permissions_body = cls(
            access_boost=access_boost,
            role=role,
            user_group_ids=user_group_ids,
            user_ids=user_ids,
        )

        return folders_update_permissions_body
