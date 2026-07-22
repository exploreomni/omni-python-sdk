from typing import Literal

CompositeFilterFiltersItemType3Type = Literal["null"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_3_TYPE_VALUES: set[CompositeFilterFiltersItemType3Type] = {
    "null",
}


def check_composite_filter_filters_item_type_3_type(value: str) -> CompositeFilterFiltersItemType3Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_3_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_3_TYPE_VALUES!r}")
