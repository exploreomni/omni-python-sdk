from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_6_type import (
    CompositeFilterFiltersItemType6Type,
    check_composite_filter_filters_item_type_6_type,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="CompositeFilterFiltersItemType6")


@_attrs_define
class CompositeFilterFiltersItemType6:
    """
    Attributes:
        type_ (CompositeFilterFiltersItemType6Type):
        user_attribute_name (str):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
    """

    type_: CompositeFilterFiltersItemType6Type
    user_attribute_name: str
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        user_attribute_name = self.user_attribute_name

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "user_attribute_name": user_attribute_name,
            }
        )
        if cancel_query_filter is not UNSET:
            field_dict["cancel_query_filter"] = cancel_query_filter
        if ignore_if_unjoinable is not UNSET:
            field_dict["ignore_if_unjoinable"] = ignore_if_unjoinable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = check_composite_filter_filters_item_type_6_type(d.pop("type"))

        user_attribute_name = d.pop("user_attribute_name")

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        composite_filter_filters_item_type_6 = cls(
            type_=type_,
            user_attribute_name=user_attribute_name,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
        )

        composite_filter_filters_item_type_6.additional_properties = d
        return composite_filter_filters_item_type_6

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
