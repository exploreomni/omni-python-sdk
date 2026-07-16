from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsUpdateBody")


@_attrs_define
class DocumentsUpdateBody:
    """
    Attributes:
        clear_existing_draft (bool | Unset): Clear existing draft before updating (for published documents with drafts)
            Default: False.
        description (None | str | Unset): Document description
        identifier (str | Unset): Optional document identifier. If omitted, an identifier is auto-generated. Must be
            unique within the organization.
        name (str | Unset): New document name
    """

    clear_existing_draft: bool | Unset = False
    description: None | str | Unset = UNSET
    identifier: str | Unset = UNSET
    name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        clear_existing_draft = self.clear_existing_draft

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        identifier = self.identifier

        name = self.name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if clear_existing_draft is not UNSET:
            field_dict["clearExistingDraft"] = clear_existing_draft
        if description is not UNSET:
            field_dict["description"] = description
        if identifier is not UNSET:
            field_dict["identifier"] = identifier
        if name is not UNSET:
            field_dict["name"] = name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        clear_existing_draft = d.pop("clearExistingDraft", UNSET)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        identifier = d.pop("identifier", UNSET)

        name = d.pop("name", UNSET)

        documents_update_body = cls(
            clear_existing_draft=clear_existing_draft,
            description=description,
            identifier=identifier,
            name=name,
        )

        documents_update_body.additional_properties = d
        return documents_update_body

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
