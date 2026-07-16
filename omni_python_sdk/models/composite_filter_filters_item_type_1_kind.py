from typing import Literal

CompositeFilterFiltersItemType1Kind = Literal["BETWEEN", "EQUALS", "GREATER_THAN", "LESS_THAN"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_KIND_VALUES: set[CompositeFilterFiltersItemType1Kind] = {
    "BETWEEN",
    "EQUALS",
    "GREATER_THAN",
    "LESS_THAN",
}


def check_composite_filter_filters_item_type_1_kind(value: str) -> CompositeFilterFiltersItemType1Kind:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_KIND_VALUES!r}")
