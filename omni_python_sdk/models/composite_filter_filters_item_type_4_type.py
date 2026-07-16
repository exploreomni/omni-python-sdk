from typing import Literal

CompositeFilterFiltersItemType4Type = Literal["boolean"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_4_TYPE_VALUES: set[CompositeFilterFiltersItemType4Type] = {
    "boolean",
}


def check_composite_filter_filters_item_type_4_type(value: str) -> CompositeFilterFiltersItemType4Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_4_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_4_TYPE_VALUES!r}")
