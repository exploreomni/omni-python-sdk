from typing import Literal

CompositeFilterFiltersItemType2Type = Literal["date"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_TYPE_VALUES: set[CompositeFilterFiltersItemType2Type] = {
    "date",
}


def check_composite_filter_filters_item_type_2_type(value: str) -> CompositeFilterFiltersItemType2Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_TYPE_VALUES!r}")
