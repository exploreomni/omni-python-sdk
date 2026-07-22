from typing import Literal

CompositeFilterFiltersItemType0Type = Literal["string"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_TYPE_VALUES: set[CompositeFilterFiltersItemType0Type] = {
    "string",
}


def check_composite_filter_filters_item_type_0_type(value: str) -> CompositeFilterFiltersItemType0Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_TYPE_VALUES!r}")
