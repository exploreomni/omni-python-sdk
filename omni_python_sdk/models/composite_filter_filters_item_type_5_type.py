from typing import Literal

CompositeFilterFiltersItemType5Type = Literal["query"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_5_TYPE_VALUES: set[CompositeFilterFiltersItemType5Type] = {
    "query",
}


def check_composite_filter_filters_item_type_5_type(value: str) -> CompositeFilterFiltersItemType5Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_5_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_5_TYPE_VALUES!r}")
