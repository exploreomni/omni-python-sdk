from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiQuerySort")


@_attrs_define
class AiQuerySort:
    """
    Attributes:
        column_name (str): Fully qualified field name to sort by (e.g., "view_name.field_name"). Example:
            order_items.total_revenue.
        sort_descending (bool): Whether to sort in descending order. Example: True.
    """

    column_name: str
    sort_descending: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        column_name = self.column_name

        sort_descending = self.sort_descending

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "column_name": column_name,
                "sort_descending": sort_descending,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        column_name = d.pop("column_name")

        sort_descending = d.pop("sort_descending")

        ai_query_sort = cls(
            column_name=column_name,
            sort_descending=sort_descending,
        )

        ai_query_sort.additional_properties = d
        return ai_query_sort

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
