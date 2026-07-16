from typing import Literal

CompositeFilterFiltersItemType1Type = Literal["number"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_TYPE_VALUES: set[CompositeFilterFiltersItemType1Type] = {
    "number",
}


def check_composite_filter_filters_item_type_1_type(value: str) -> CompositeFilterFiltersItemType1Type:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_1_TYPE_VALUES!r}")
