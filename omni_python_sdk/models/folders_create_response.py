from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.folders_create_response_scope import FoldersCreateResponseScope, check_folders_create_response_scope

T = TypeVar("T", bound="FoldersCreateResponse")


@_attrs_define
class FoldersCreateResponse:
    """
    Attributes:
        id (UUID): ID of the created folder
        name (str): Name of the created folder
        owner_id (UUID): User ID of the folder owner
        path (str): Full path to the folder
        scope (FoldersCreateResponseScope): Share scope of the folder
    """

    id: UUID
    name: str
    owner_id: UUID
    path: str
    scope: FoldersCreateResponseScope
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        name = self.name

        owner_id = str(self.owner_id)

        path = self.path

        scope: str = self.scope

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "ownerId": owner_id,
                "path": path,
                "scope": scope,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = UUID(d.pop("id"))

        name = d.pop("name")

        owner_id = UUID(d.pop("ownerId"))

        path = d.pop("path")

        scope = check_folders_create_response_scope(d.pop("scope"))

        folders_create_response = cls(
            id=id,
            name=name,
            owner_id=owner_id,
            path=path,
            scope=scope,
        )

        folders_create_response.additional_properties = d
        return folders_create_response

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
