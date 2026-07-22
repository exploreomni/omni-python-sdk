from typing import Literal

CompositeFilterFiltersItemType2Kind = Literal[
    "BEFORE",
    "BETWEEN",
    "IS_AT_HOUR_OF_DAY",
    "IS_IN_MONTH_OF_YEAR",
    "IS_IN_QUARTER_OF_YEAR",
    "IS_IN_WEEK_OF_YEAR",
    "IS_ON_DAY_OF_MONTH",
    "IS_ON_DAY_OF_QUARTER",
    "IS_ON_DAY_OF_WEEK",
    "IS_ON_DAY_OF_YEAR",
    "ON_OR_AFTER",
    "QUERY_OFFSET",
    "TIME_FOR_INTERVAL_DURATION",
    "TIME_FOR_UNIT_DURATION",
]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_KIND_VALUES: set[CompositeFilterFiltersItemType2Kind] = {
    "BEFORE",
    "BETWEEN",
    "IS_AT_HOUR_OF_DAY",
    "IS_IN_MONTH_OF_YEAR",
    "IS_IN_QUARTER_OF_YEAR",
    "IS_IN_WEEK_OF_YEAR",
    "IS_ON_DAY_OF_MONTH",
    "IS_ON_DAY_OF_QUARTER",
    "IS_ON_DAY_OF_WEEK",
    "IS_ON_DAY_OF_YEAR",
    "ON_OR_AFTER",
    "QUERY_OFFSET",
    "TIME_FOR_INTERVAL_DURATION",
    "TIME_FOR_UNIT_DURATION",
}


def check_composite_filter_filters_item_type_2_kind(value: str) -> CompositeFilterFiltersItemType2Kind:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_KIND_VALUES!r}")
