from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FoldersUpdateBody")


@_attrs_define
class FoldersUpdateBody:
    """
    Attributes:
        name (str | Unset): New display name for the folder Example: Q1 Reports.
        path (str | Unset): New URL path segment for the folder (alphanumeric and dashes only). This is only the
            folder's own segment, not the full hierarchical path. Example: q1-reports.
        resolve_path_conflict (bool | Unset): When true, automatically resolves path collisions with existing folders by
            appending a numeric suffix (e.g., my-path-1). When false (default), returns 409 Conflict if the path is already
            taken. Does not apply to reserved paths, which are always rejected with 400. Default: False.
    """

    name: str | Unset = UNSET
    path: str | Unset = UNSET
    resolve_path_conflict: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        path = self.path

        resolve_path_conflict = self.resolve_path_conflict

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if path is not UNSET:
            field_dict["path"] = path
        if resolve_path_conflict is not UNSET:
            field_dict["resolvePathConflict"] = resolve_path_conflict

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name", UNSET)

        path = d.pop("path", UNSET)

        resolve_path_conflict = d.pop("resolvePathConflict", UNSET)

        folders_update_body = cls(
            name=name,
            path=path,
            resolve_path_conflict=resolve_path_conflict,
        )

        folders_update_body.additional_properties = d
        return folders_update_body

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
