from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentsV2PatchDraftResponse")


@_attrs_define
class DocumentsV2PatchDraftResponse:
    """
    Attributes:
        description (None | str): Document description.
        draft_identifier (str): Identifier of the draft the patch was applied to.
        identifier (str): Published document identifier the draft targets.
        name (str): Document name.
    """

    description: None | str
    draft_identifier: str
    identifier: str
    name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description: None | str
        description = self.description

        draft_identifier = self.draft_identifier

        identifier = self.identifier

        name = self.name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "description": description,
                "draftIdentifier": draft_identifier,
                "identifier": identifier,
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        draft_identifier = d.pop("draftIdentifier")

        identifier = d.pop("identifier")

        name = d.pop("name")

        documents_v2_patch_draft_response = cls(
            description=description,
            draft_identifier=draft_identifier,
            identifier=identifier,
            name=name,
        )

        documents_v2_patch_draft_response.additional_properties = d
        return documents_v2_patch_draft_response

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
