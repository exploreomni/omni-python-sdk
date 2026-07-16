from typing import Literal

CompositeFilterFiltersItemType0Kind = Literal["CONTAINS", "ENDS_WITH", "EQUALS", "IS_EMPTY", "SQL_LIKE", "STARTS_WITH"]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_KIND_VALUES: set[CompositeFilterFiltersItemType0Kind] = {
    "CONTAINS",
    "ENDS_WITH",
    "EQUALS",
    "IS_EMPTY",
    "SQL_LIKE",
    "STARTS_WITH",
}


def check_composite_filter_filters_item_type_0_kind(value: str) -> CompositeFilterFiltersItemType0Kind:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_0_KIND_VALUES!r}")
