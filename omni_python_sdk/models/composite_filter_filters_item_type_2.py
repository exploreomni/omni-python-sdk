from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_filters_item_type_2_kind import (
    CompositeFilterFiltersItemType2Kind,
    check_composite_filter_filters_item_type_2_kind,
)
from ..models.composite_filter_filters_item_type_2_type import (
    CompositeFilterFiltersItemType2Type,
    check_composite_filter_filters_item_type_2_type,
)
from ..models.composite_filter_filters_item_type_2_ui_type_type_1 import (
    CompositeFilterFiltersItemType2UiTypeType1,
    check_composite_filter_filters_item_type_2_ui_type_type_1,
)
from ..models.composite_filter_filters_item_type_2_ui_type_type_2_type_1 import (
    CompositeFilterFiltersItemType2UiTypeType2Type1,
    check_composite_filter_filters_item_type_2_ui_type_type_2_type_1,
)
from ..models.composite_filter_filters_item_type_2_ui_type_type_3_type_1 import (
    CompositeFilterFiltersItemType2UiTypeType3Type1,
    check_composite_filter_filters_item_type_2_ui_type_type_3_type_1,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="CompositeFilterFiltersItemType2")


@_attrs_define
class CompositeFilterFiltersItemType2:
    """
    Attributes:
        kind (CompositeFilterFiltersItemType2Kind):
        type_ (CompositeFilterFiltersItemType2Type):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        is_fiscal (bool | Unset):
        is_negative (bool | None | Unset):
        left_side (None | str | Unset):
        offset_interval_string (None | str | Unset):
        right_side (None | str | Unset):
        ui_type (CompositeFilterFiltersItemType2UiTypeType1 | CompositeFilterFiltersItemType2UiTypeType2Type1 |
            CompositeFilterFiltersItemType2UiTypeType3Type1 | None | Unset):
    """

    kind: CompositeFilterFiltersItemType2Kind
    type_: CompositeFilterFiltersItemType2Type
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    is_fiscal: bool | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    left_side: None | str | Unset = UNSET
    offset_interval_string: None | str | Unset = UNSET
    right_side: None | str | Unset = UNSET
    ui_type: (
        CompositeFilterFiltersItemType2UiTypeType1
        | CompositeFilterFiltersItemType2UiTypeType2Type1
        | CompositeFilterFiltersItemType2UiTypeType3Type1
        | None
        | Unset
    ) = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind: str = self.kind

        type_: str = self.type_

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        is_fiscal = self.is_fiscal

        is_negative: bool | None | Unset
        if isinstance(self.is_negative, Unset):
            is_negative = UNSET
        else:
            is_negative = self.is_negative

        left_side: None | str | Unset
        if isinstance(self.left_side, Unset):
            left_side = UNSET
        else:
            left_side = self.left_side

        offset_interval_string: None | str | Unset
        if isinstance(self.offset_interval_string, Unset):
            offset_interval_string = UNSET
        else:
            offset_interval_string = self.offset_interval_string

        right_side: None | str | Unset
        if isinstance(self.right_side, Unset):
            right_side = UNSET
        else:
            right_side = self.right_side

        ui_type: None | str | Unset
        if isinstance(self.ui_type, Unset):
            ui_type = UNSET
        elif isinstance(self.ui_type, str):
            ui_type = self.ui_type
        elif isinstance(self.ui_type, str):
            ui_type = self.ui_type
        elif isinstance(self.ui_type, str):
            ui_type = self.ui_type
        else:
            ui_type = self.ui_type

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "type": type_,
            }
        )
        if cancel_query_filter is not UNSET:
            field_dict["cancel_query_filter"] = cancel_query_filter
        if ignore_if_unjoinable is not UNSET:
            field_dict["ignore_if_unjoinable"] = ignore_if_unjoinable
        if is_fiscal is not UNSET:
            field_dict["isFiscal"] = is_fiscal
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative
        if left_side is not UNSET:
            field_dict["left_side"] = left_side
        if offset_interval_string is not UNSET:
            field_dict["offset_interval_string"] = offset_interval_string
        if right_side is not UNSET:
            field_dict["right_side"] = right_side
        if ui_type is not UNSET:
            field_dict["ui_type"] = ui_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = check_composite_filter_filters_item_type_2_kind(d.pop("kind"))

        type_ = check_composite_filter_filters_item_type_2_type(d.pop("type"))

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        is_fiscal = d.pop("isFiscal", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        def _parse_left_side(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        left_side = _parse_left_side(d.pop("left_side", UNSET))

        def _parse_offset_interval_string(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        offset_interval_string = _parse_offset_interval_string(d.pop("offset_interval_string", UNSET))

        def _parse_right_side(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        right_side = _parse_right_side(d.pop("right_side", UNSET))

        def _parse_ui_type(
            data: object,
        ) -> (
            CompositeFilterFiltersItemType2UiTypeType1
            | CompositeFilterFiltersItemType2UiTypeType2Type1
            | CompositeFilterFiltersItemType2UiTypeType3Type1
            | None
            | Unset
        ):
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ui_type_type_1 = check_composite_filter_filters_item_type_2_ui_type_type_1(data)

                return ui_type_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ui_type_type_2_type_1 = check_composite_filter_filters_item_type_2_ui_type_type_2_type_1(data)

                return ui_type_type_2_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ui_type_type_3_type_1 = check_composite_filter_filters_item_type_2_ui_type_type_3_type_1(data)

                return ui_type_type_3_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                CompositeFilterFiltersItemType2UiTypeType1
                | CompositeFilterFiltersItemType2UiTypeType2Type1
                | CompositeFilterFiltersItemType2UiTypeType3Type1
                | None
                | Unset,
                data,
            )

        ui_type = _parse_ui_type(d.pop("ui_type", UNSET))

        composite_filter_filters_item_type_2 = cls(
            kind=kind,
            type_=type_,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            is_fiscal=is_fiscal,
            is_negative=is_negative,
            left_side=left_side,
            offset_interval_string=offset_interval_string,
            right_side=right_side,
            ui_type=ui_type,
        )

        composite_filter_filters_item_type_2.additional_properties = d
        return composite_filter_filters_item_type_2

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
