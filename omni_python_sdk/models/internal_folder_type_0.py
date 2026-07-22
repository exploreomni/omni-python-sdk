from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.content_share_scope import ContentShareScope, check_content_share_scope

T = TypeVar("T", bound="InternalFolderType0")


@_attrs_define
class InternalFolderType0:
    """Parent folder

    Attributes:
        id (str): Folder ID
        name (str): Folder name
        path (str): Folder path
        scope (ContentShareScope): Content access scope
    """

    id: str
    name: str
    path: str
    scope: ContentShareScope
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        path = self.path

        scope: str = self.scope

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "path": path,
                "scope": scope,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        path = d.pop("path")

        scope = check_content_share_scope(d.pop("scope"))

        internal_folder_type_0 = cls(
            id=id,
            name=name,
            path=path,
            scope=scope,
        )

        internal_folder_type_0.additional_properties = d
        return internal_folder_type_0

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
