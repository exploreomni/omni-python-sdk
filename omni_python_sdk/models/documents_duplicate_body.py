from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.documents_duplicate_body_scope import DocumentsDuplicateBodyScope, check_documents_duplicate_body_scope
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsDuplicateBody")


@_attrs_define
class DocumentsDuplicateBody:
    """
    Attributes:
        name (str): Name for the duplicated document
        folder_path (None | str | Unset): Destination folder path (null for root)
        scope (DocumentsDuplicateBodyScope | Unset): Access scope for the duplicated document
    """

    name: str
    folder_path: None | str | Unset = UNSET
    scope: DocumentsDuplicateBodyScope | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        folder_path: None | str | Unset
        if isinstance(self.folder_path, Unset):
            folder_path = UNSET
        else:
            folder_path = self.folder_path

        scope: str | Unset = UNSET
        if not isinstance(self.scope, Unset):
            scope = self.scope

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if folder_path is not UNSET:
            field_dict["folderPath"] = folder_path
        if scope is not UNSET:
            field_dict["scope"] = scope

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        def _parse_folder_path(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        folder_path = _parse_folder_path(d.pop("folderPath", UNSET))

        _scope = d.pop("scope", UNSET)
        scope: DocumentsDuplicateBodyScope | Unset
        if isinstance(_scope, Unset):
            scope = UNSET
        else:
            scope = check_documents_duplicate_body_scope(_scope)

        documents_duplicate_body = cls(
            name=name,
            folder_path=folder_path,
            scope=scope,
        )

        documents_duplicate_body.additional_properties = d
        return documents_duplicate_body

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
