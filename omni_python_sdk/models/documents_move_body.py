from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.documents_move_body_scope import DocumentsMoveBodyScope, check_documents_move_body_scope
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsMoveBody")


@_attrs_define
class DocumentsMoveBody:
    """
    Attributes:
        folder_path (None | str): Destination folder path (null for root)
        scope (DocumentsMoveBodyScope | Unset): Access scope for the document
    """

    folder_path: None | str
    scope: DocumentsMoveBodyScope | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        folder_path: None | str
        folder_path = self.folder_path

        scope: str | Unset = UNSET
        if not isinstance(self.scope, Unset):
            scope = self.scope

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "folderPath": folder_path,
            }
        )
        if scope is not UNSET:
            field_dict["scope"] = scope

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_folder_path(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        folder_path = _parse_folder_path(d.pop("folderPath"))

        _scope = d.pop("scope", UNSET)
        scope: DocumentsMoveBodyScope | Unset
        if isinstance(_scope, Unset):
            scope = UNSET
        else:
            scope = check_documents_move_body_scope(_scope)

        documents_move_body = cls(
            folder_path=folder_path,
            scope=scope,
        )

        documents_move_body.additional_properties = d
        return documents_move_body

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
