from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_0_kind import (
    CompositeFilterFiltersItemType0Kind,
    check_composite_filter_filters_item_type_0_kind,
)
from ..models.composite_filter_filters_item_type_0_type import (
    CompositeFilterFiltersItemType0Type,
    check_composite_filter_filters_item_type_0_type,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_filter_filters_item_type_0_applied_labels import (
        CompositeFilterFiltersItemType0AppliedLabels,
    )


T = TypeVar("T", bound="CompositeFilterFiltersItemType0")


@_attrs_define
class CompositeFilterFiltersItemType0:
    """
    Attributes:
        kind (CompositeFilterFiltersItemType0Kind):
        type_ (CompositeFilterFiltersItemType0Type):
        values (list[str]):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        applied_labels (CompositeFilterFiltersItemType0AppliedLabels | Unset):
        case_insensitive (bool | Unset):
        is_negative (bool | None | Unset):
    """

    kind: CompositeFilterFiltersItemType0Kind
    type_: CompositeFilterFiltersItemType0Type
    values: list[str]
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    applied_labels: CompositeFilterFiltersItemType0AppliedLabels | Unset = UNSET
    case_insensitive: bool | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind: str = self.kind

        type_: str = self.type_

        values = self.values

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        applied_labels: dict[str, Any] | Unset = UNSET
        if not isinstance(self.applied_labels, Unset):
            applied_labels = self.applied_labels.to_dict()

        case_insensitive = self.case_insensitive

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
        if applied_labels is not UNSET:
            field_dict["appliedLabels"] = applied_labels
        if case_insensitive is not UNSET:
            field_dict["case_insensitive"] = case_insensitive
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_filter_filters_item_type_0_applied_labels import (
            CompositeFilterFiltersItemType0AppliedLabels,
        )

        d = dict(src_dict)
        kind = check_composite_filter_filters_item_type_0_kind(d.pop("kind"))

        type_ = check_composite_filter_filters_item_type_0_type(d.pop("type"))

        values = cast(list[str], d.pop("values"))

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        _applied_labels = d.pop("appliedLabels", UNSET)
        applied_labels: CompositeFilterFiltersItemType0AppliedLabels | Unset
        if isinstance(_applied_labels, Unset):
            applied_labels = UNSET
        else:
            applied_labels = CompositeFilterFiltersItemType0AppliedLabels.from_dict(_applied_labels)

        case_insensitive = d.pop("case_insensitive", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        composite_filter_filters_item_type_0 = cls(
            kind=kind,
            type_=type_,
            values=values,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            applied_labels=applied_labels,
            case_insensitive=case_insensitive,
            is_negative=is_negative,
        )

        composite_filter_filters_item_type_0.additional_properties = d
        return composite_filter_filters_item_type_0

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
