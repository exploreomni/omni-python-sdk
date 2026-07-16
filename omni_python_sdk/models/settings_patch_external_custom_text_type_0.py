from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SettingsPatchExternalCustomTextType0")


@_attrs_define
class SettingsPatchExternalCustomTextType0:
    """Custom text replacing default UI strings on the dashboard, e.g. when queries error or return no results.

    Attributes:
        query_error (str | Unset): Custom text shown when a query errors, replacing the default error text.
        query_no_results (str | Unset): Custom text shown when a query returns no results, replacing the default empty
            state.
    """

    query_error: str | Unset = UNSET
    query_no_results: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query_error = self.query_error

        query_no_results = self.query_no_results

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if query_error is not UNSET:
            field_dict["queryError"] = query_error
        if query_no_results is not UNSET:
            field_dict["queryNoResults"] = query_no_results

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query_error = d.pop("queryError", UNSET)

        query_no_results = d.pop("queryNoResults", UNSET)

        settings_patch_external_custom_text_type_0 = cls(
            query_error=query_error,
            query_no_results=query_no_results,
        )

        settings_patch_external_custom_text_type_0.additional_properties = d
        return settings_patch_external_custom_text_type_0

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
