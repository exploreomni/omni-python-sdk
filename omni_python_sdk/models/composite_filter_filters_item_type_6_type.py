from typing import Literal

CompositeFilterFiltersItemType6Type = Literal["user_attribute"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_6_TYPE_VALUES: set[CompositeFilterFiltersItemType6Type] = {
    "user_attribute",
}


def check_composite_filter_filters_item_type_6_type(value: str) -> CompositeFilterFiltersItemType6Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_6_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_6_TYPE_VALUES!r}")
