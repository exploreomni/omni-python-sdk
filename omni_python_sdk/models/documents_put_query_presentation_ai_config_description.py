from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsPutQueryPresentationAiConfigDescription")


@_attrs_define
class DocumentsPutQueryPresentationAiConfigDescription:
    """
    Attributes:
        ai_context (str | Unset):
        enabled (bool | Unset):
    """

    ai_context: str | Unset = UNSET
    enabled: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ai_context = self.ai_context

        enabled = self.enabled

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if ai_context is not UNSET:
            field_dict["aiContext"] = ai_context
        if enabled is not UNSET:
            field_dict["enabled"] = enabled

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ai_context = d.pop("aiContext", UNSET)

        enabled = d.pop("enabled", UNSET)

        documents_put_query_presentation_ai_config_description = cls(
            ai_context=ai_context,
            enabled=enabled,
        )

        documents_put_query_presentation_ai_config_description.additional_properties = d
        return documents_put_query_presentation_ai_config_description

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
