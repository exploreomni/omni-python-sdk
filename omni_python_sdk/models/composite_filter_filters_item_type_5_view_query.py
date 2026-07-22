from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_filter_filters_item_type_5_view_query_filters import (
        CompositeFilterFiltersItemType5ViewQueryFilters,
    )


T = TypeVar("T", bound="CompositeFilterFiltersItemType5ViewQuery")


@_attrs_define
class CompositeFilterFiltersItemType5ViewQuery:
    """
    Attributes:
        fields (list[str]):
        filters (CompositeFilterFiltersItemType5ViewQueryFilters | Unset):
        limit (float | Unset):
        sorts (list[Any] | Unset):
        table (str | Unset):
    """

    fields: list[str]
    filters: CompositeFilterFiltersItemType5ViewQueryFilters | Unset = UNSET
    limit: float | Unset = UNSET
    sorts: list[Any] | Unset = UNSET
    table: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filters, Unset):
            filters = self.filters.to_dict()

        limit = self.limit

        sorts: list[Any] | Unset = UNSET
        if not isinstance(self.sorts, Unset):
            sorts = self.sorts

        table = self.table

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "fields": fields,
            }
        )
        if filters is not UNSET:
            field_dict["filters"] = filters
        if limit is not UNSET:
            field_dict["limit"] = limit
        if sorts is not UNSET:
            field_dict["sorts"] = sorts
        if table is not UNSET:
            field_dict["table"] = table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_filter_filters_item_type_5_view_query_filters import (
            CompositeFilterFiltersItemType5ViewQueryFilters,
        )

        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        _filters = d.pop("filters", UNSET)
        filters: CompositeFilterFiltersItemType5ViewQueryFilters | Unset
        if isinstance(_filters, Unset):
            filters = UNSET
        else:
            filters = CompositeFilterFiltersItemType5ViewQueryFilters.from_dict(_filters)

        limit = d.pop("limit", UNSET)

        sorts = cast(list[Any], d.pop("sorts", UNSET))

        table = d.pop("table", UNSET)

        composite_filter_filters_item_type_5_view_query = cls(
            fields=fields,
            filters=filters,
            limit=limit,
            sorts=sorts,
            table=table,
        )

        composite_filter_filters_item_type_5_view_query.additional_properties = d
        return composite_filter_filters_item_type_5_view_query

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
