from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_4_type import (
    CompositeFilterFiltersItemType4Type,
    check_composite_filter_filters_item_type_4_type,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="CompositeFilterFiltersItemType4")


@_attrs_define
class CompositeFilterFiltersItemType4:
    """
    Attributes:
        type_ (CompositeFilterFiltersItemType4Type):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        is_negative (bool | None | Unset):
        treat_nulls_as_false (bool | Unset):
    """

    type_: CompositeFilterFiltersItemType4Type
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    treat_nulls_as_false: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        is_negative: bool | None | Unset
        if isinstance(self.is_negative, Unset):
            is_negative = UNSET
        else:
            is_negative = self.is_negative

        treat_nulls_as_false = self.treat_nulls_as_false

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if cancel_query_filter is not UNSET:
            field_dict["cancel_query_filter"] = cancel_query_filter
        if ignore_if_unjoinable is not UNSET:
            field_dict["ignore_if_unjoinable"] = ignore_if_unjoinable
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative
        if treat_nulls_as_false is not UNSET:
            field_dict["treat_nulls_as_false"] = treat_nulls_as_false

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = check_composite_filter_filters_item_type_4_type(d.pop("type"))

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        treat_nulls_as_false = d.pop("treat_nulls_as_false", UNSET)

        composite_filter_filters_item_type_4 = cls(
            type_=type_,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            is_negative=is_negative,
            treat_nulls_as_false=treat_nulls_as_false,
        )

        composite_filter_filters_item_type_4.additional_properties = d
        return composite_filter_filters_item_type_4

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
