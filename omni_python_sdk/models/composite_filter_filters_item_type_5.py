from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_5_type import (
    CompositeFilterFiltersItemType5Type,
    check_composite_filter_filters_item_type_5_type,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_filter_filters_item_type_5_view_query import CompositeFilterFiltersItemType5ViewQuery


T = TypeVar("T", bound="CompositeFilterFiltersItemType5")


@_attrs_define
class CompositeFilterFiltersItemType5:
    """
    Attributes:
        type_ (CompositeFilterFiltersItemType5Type):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        disregard_limit (bool | Unset):
        field_name (str | Unset):
        is_negative (bool | None | Unset):
        query_id (str | Unset):
        view_query (CompositeFilterFiltersItemType5ViewQuery | Unset):
    """

    type_: CompositeFilterFiltersItemType5Type
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    disregard_limit: bool | Unset = UNSET
    field_name: str | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    query_id: str | Unset = UNSET
    view_query: CompositeFilterFiltersItemType5ViewQuery | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        disregard_limit = self.disregard_limit

        field_name = self.field_name

        is_negative: bool | None | Unset
        if isinstance(self.is_negative, Unset):
            is_negative = UNSET
        else:
            is_negative = self.is_negative

        query_id = self.query_id

        view_query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.view_query, Unset):
            view_query = self.view_query.to_dict()

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
        if disregard_limit is not UNSET:
            field_dict["disregard_limit"] = disregard_limit
        if field_name is not UNSET:
            field_dict["field_name"] = field_name
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative
        if query_id is not UNSET:
            field_dict["query_id"] = query_id
        if view_query is not UNSET:
            field_dict["view_query"] = view_query

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_filter_filters_item_type_5_view_query import CompositeFilterFiltersItemType5ViewQuery

        d = dict(src_dict)
        type_ = check_composite_filter_filters_item_type_5_type(d.pop("type"))

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        disregard_limit = d.pop("disregard_limit", UNSET)

        field_name = d.pop("field_name", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        query_id = d.pop("query_id", UNSET)

        _view_query = d.pop("view_query", UNSET)
        view_query: CompositeFilterFiltersItemType5ViewQuery | Unset
        if isinstance(_view_query, Unset):
            view_query = UNSET
        else:
            view_query = CompositeFilterFiltersItemType5ViewQuery.from_dict(_view_query)

        composite_filter_filters_item_type_5 = cls(
            type_=type_,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            disregard_limit=disregard_limit,
            field_name=field_name,
            is_negative=is_negative,
            query_id=query_id,
            view_query=view_query,
        )

        composite_filter_filters_item_type_5.additional_properties = d
        return composite_filter_filters_item_type_5

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
