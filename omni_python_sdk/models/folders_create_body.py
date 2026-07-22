from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.folders_create_body_scope import FoldersCreateBodyScope, check_folders_create_body_scope
from ..types import UNSET, Unset

T = TypeVar("T", bound="FoldersCreateBody")


@_attrs_define
class FoldersCreateBody:
    """
    Attributes:
        name (str): Name of the folder to create Example: My New Folder.
        parent_folder_id (UUID | Unset): Parent folder ID (omit to create at root level)
        scope (FoldersCreateBodyScope | Unset): Share scope for the folder
        user_id (UUID | Unset): User ID to create the folder as (for org-scoped API keys only)
    """

    name: str
    parent_folder_id: UUID | Unset = UNSET
    scope: FoldersCreateBodyScope | Unset = UNSET
    user_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        parent_folder_id: str | Unset = UNSET
        if not isinstance(self.parent_folder_id, Unset):
            parent_folder_id = str(self.parent_folder_id)

        scope: str | Unset = UNSET
        if not isinstance(self.scope, Unset):
            scope = self.scope

        user_id: str | Unset = UNSET
        if not isinstance(self.user_id, Unset):
            user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if parent_folder_id is not UNSET:
            field_dict["parentFolderId"] = parent_folder_id
        if scope is not UNSET:
            field_dict["scope"] = scope
        if user_id is not UNSET:
            field_dict["userId"] = user_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        _parent_folder_id = d.pop("parentFolderId", UNSET)
        parent_folder_id: UUID | Unset
        if isinstance(_parent_folder_id, Unset):
            parent_folder_id = UNSET
        else:
            parent_folder_id = UUID(_parent_folder_id)

        _scope = d.pop("scope", UNSET)
        scope: FoldersCreateBodyScope | Unset
        if isinstance(_scope, Unset):
            scope = UNSET
        else:
            scope = check_folders_create_body_scope(_scope)

        _user_id = d.pop("userId", UNSET)
        user_id: UUID | Unset
        if isinstance(_user_id, Unset):
            user_id = UNSET
        else:
            user_id = UUID(_user_id)

        folders_create_body = cls(
            name=name,
            parent_folder_id=parent_folder_id,
            scope=scope,
            user_id=user_id,
        )

        folders_create_body.additional_properties = d
        return folders_create_body

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
