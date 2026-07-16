from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_1_kind import (
    CompositeFilterFiltersItemType1Kind,
    check_composite_filter_filters_item_type_1_kind,
)
from ..models.composite_filter_filters_item_type_1_type import (
    CompositeFilterFiltersItemType1Type,
    check_composite_filter_filters_item_type_1_type,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="CompositeFilterFiltersItemType1")


@_attrs_define
class CompositeFilterFiltersItemType1:
    """
    Attributes:
        kind (CompositeFilterFiltersItemType1Kind):
        type_ (CompositeFilterFiltersItemType1Type):
        values (list[float | str]):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        is_inclusive (bool | Unset):
        is_negative (bool | None | Unset):
    """

    kind: CompositeFilterFiltersItemType1Kind
    type_: CompositeFilterFiltersItemType1Type
    values: list[float | str]
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    is_inclusive: bool | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind: str = self.kind

        type_: str = self.type_

        values = []
        for values_item_data in self.values:
            values_item: float | str
            values_item = values_item_data
            values.append(values_item)

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        is_inclusive = self.is_inclusive

        is_negative: bool | None | Unset
        if isinstance(self.is_negative, Unset):
            is_negative = UNSET
        else:
            is_negative = self.is_negative

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "type": type_,
                "values": values,
            }
        )
        if cancel_query_filter is not UNSET:
            field_dict["cancel_query_filter"] = cancel_query_filter
        if ignore_if_unjoinable is not UNSET:
            field_dict["ignore_if_unjoinable"] = ignore_if_unjoinable
        if is_inclusive is not UNSET:
            field_dict["is_inclusive"] = is_inclusive
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = check_composite_filter_filters_item_type_1_kind(d.pop("kind"))

        type_ = check_composite_filter_filters_item_type_1_type(d.pop("type"))

        values = []
        _values = d.pop("values")
        for values_item_data in _values:

            def _parse_values_item(data: object) -> float | str:
                return cast(float | str, data)

            values_item = _parse_values_item(values_item_data)

            values.append(values_item)

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        is_inclusive = d.pop("is_inclusive", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        composite_filter_filters_item_type_1 = cls(
            kind=kind,
            type_=type_,
            values=values,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            is_inclusive=is_inclusive,
            is_negative=is_negative,
        )

        composite_filter_filters_item_type_1.additional_properties = d
        return composite_filter_filters_item_type_1

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
