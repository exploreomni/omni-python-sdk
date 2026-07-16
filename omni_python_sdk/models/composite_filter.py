from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.composite_filter_conjunction import CompositeFilterConjunction, check_composite_filter_conjunction
from ..models.composite_filter_type import CompositeFilterType, check_composite_filter_type
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composite_filter_filters_item_type_0 import CompositeFilterFiltersItemType0
    from ..models.composite_filter_filters_item_type_1 import CompositeFilterFiltersItemType1
    from ..models.composite_filter_filters_item_type_2 import CompositeFilterFiltersItemType2
    from ..models.composite_filter_filters_item_type_3 import CompositeFilterFiltersItemType3
    from ..models.composite_filter_filters_item_type_4 import CompositeFilterFiltersItemType4
    from ..models.composite_filter_filters_item_type_5 import CompositeFilterFiltersItemType5
    from ..models.composite_filter_filters_item_type_6 import CompositeFilterFiltersItemType6


T = TypeVar("T", bound="CompositeFilter")


@_attrs_define
class CompositeFilter:
    """
    Attributes:
        conjunction (CompositeFilterConjunction):
        filters (list[CompositeFilter | CompositeFilterFiltersItemType0 | CompositeFilterFiltersItemType1 |
            CompositeFilterFiltersItemType2 | CompositeFilterFiltersItemType3 | CompositeFilterFiltersItemType4 |
            CompositeFilterFiltersItemType5 | CompositeFilterFiltersItemType6]): Child filters — each a simple filter or
            another composite filter. Recursive; see the dashboard-filters reference for the full grammar.
        type_ (CompositeFilterType):
        cancel_query_filter (bool | Unset):
        ignore_if_unjoinable (bool | Unset):
        is_negative (bool | None | Unset):
    """

    conjunction: CompositeFilterConjunction
    filters: list[
        CompositeFilter
        | CompositeFilterFiltersItemType0
        | CompositeFilterFiltersItemType1
        | CompositeFilterFiltersItemType2
        | CompositeFilterFiltersItemType3
        | CompositeFilterFiltersItemType4
        | CompositeFilterFiltersItemType5
        | CompositeFilterFiltersItemType6
    ]
    type_: CompositeFilterType
    cancel_query_filter: bool | Unset = UNSET
    ignore_if_unjoinable: bool | Unset = UNSET
    is_negative: bool | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.composite_filter_filters_item_type_0 import CompositeFilterFiltersItemType0
        from ..models.composite_filter_filters_item_type_1 import CompositeFilterFiltersItemType1
        from ..models.composite_filter_filters_item_type_2 import CompositeFilterFiltersItemType2
        from ..models.composite_filter_filters_item_type_3 import CompositeFilterFiltersItemType3
        from ..models.composite_filter_filters_item_type_4 import CompositeFilterFiltersItemType4
        from ..models.composite_filter_filters_item_type_5 import CompositeFilterFiltersItemType5
        from ..models.composite_filter_filters_item_type_6 import CompositeFilterFiltersItemType6

        conjunction: str = self.conjunction

        filters = []
        for filters_item_data in self.filters:
            filters_item: dict[str, Any]
            if isinstance(filters_item_data, CompositeFilterFiltersItemType0):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType1):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType2):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType3):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType4):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType5):
                filters_item = filters_item_data.to_dict()
            elif isinstance(filters_item_data, CompositeFilterFiltersItemType6):
                filters_item = filters_item_data.to_dict()
            else:
                filters_item = filters_item_data.to_dict()

            filters.append(filters_item)

        type_: str = self.type_

        cancel_query_filter = self.cancel_query_filter

        ignore_if_unjoinable = self.ignore_if_unjoinable

        is_negative: bool | None | Unset
        if isinstance(self.is_negative, Unset):
            is_negative = UNSET
        else:
            is_negative = self.is_negative

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conjunction": conjunction,
                "filters": filters,
                "type": type_,
            }
        )
        if cancel_query_filter is not UNSET:
            field_dict["cancel_query_filter"] = cancel_query_filter
        if ignore_if_unjoinable is not UNSET:
            field_dict["ignore_if_unjoinable"] = ignore_if_unjoinable
        if is_negative is not UNSET:
            field_dict["is_negative"] = is_negative

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composite_filter_filters_item_type_0 import CompositeFilterFiltersItemType0
        from ..models.composite_filter_filters_item_type_1 import CompositeFilterFiltersItemType1
        from ..models.composite_filter_filters_item_type_2 import CompositeFilterFiltersItemType2
        from ..models.composite_filter_filters_item_type_3 import CompositeFilterFiltersItemType3
        from ..models.composite_filter_filters_item_type_4 import CompositeFilterFiltersItemType4
        from ..models.composite_filter_filters_item_type_5 import CompositeFilterFiltersItemType5
        from ..models.composite_filter_filters_item_type_6 import CompositeFilterFiltersItemType6

        d = dict(src_dict)
        conjunction = check_composite_filter_conjunction(d.pop("conjunction"))

        filters = []
        _filters = d.pop("filters")
        for filters_item_data in _filters:

            def _parse_filters_item(
                data: object,
            ) -> (
                CompositeFilter
                | CompositeFilterFiltersItemType0
                | CompositeFilterFiltersItemType1
                | CompositeFilterFiltersItemType2
                | CompositeFilterFiltersItemType3
                | CompositeFilterFiltersItemType4
                | CompositeFilterFiltersItemType5
                | CompositeFilterFiltersItemType6
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_0 = CompositeFilterFiltersItemType0.from_dict(data)

                    return filters_item_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_1 = CompositeFilterFiltersItemType1.from_dict(data)

                    return filters_item_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_2 = CompositeFilterFiltersItemType2.from_dict(data)

                    return filters_item_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_3 = CompositeFilterFiltersItemType3.from_dict(data)

                    return filters_item_type_3
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_4 = CompositeFilterFiltersItemType4.from_dict(data)

                    return filters_item_type_4
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_5 = CompositeFilterFiltersItemType5.from_dict(data)

                    return filters_item_type_5
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    filters_item_type_6 = CompositeFilterFiltersItemType6.from_dict(data)

                    return filters_item_type_6
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                filters_item_type_7 = CompositeFilter.from_dict(data)

                return filters_item_type_7

            filters_item = _parse_filters_item(filters_item_data)

            filters.append(filters_item)

        type_ = check_composite_filter_type(d.pop("type"))

        cancel_query_filter = d.pop("cancel_query_filter", UNSET)

        ignore_if_unjoinable = d.pop("ignore_if_unjoinable", UNSET)

        def _parse_is_negative(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        is_negative = _parse_is_negative(d.pop("is_negative", UNSET))

        composite_filter = cls(
            conjunction=conjunction,
            filters=filters,
            type_=type_,
            cancel_query_filter=cancel_query_filter,
            ignore_if_unjoinable=ignore_if_unjoinable,
            is_negative=is_negative,
        )

        composite_filter.additional_properties = d
        return composite_filter

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
