from typing import Literal

CompositeFilterFiltersItemType2UiTypeType1 = Literal[
    "ANY_TIME",
    "BEFORE",
    "BETWEEN",
    "CUSTOM",
    "DAY",
    "IS_IN_THE_FISCAL_QUARTER",
    "IS_IN_THE_FISCAL_YEAR",
    "IS_IN_THE_MONTH",
    "IS_IN_THE_QUARTER",
    "IS_ON_DAY_OF_WEEK",
    "MONTH_OF_YEAR",
    "ON_OR_AFTER",
    "PAST",
    "TIME_FOR_INTERVAL_DURATION",
    "TIME_FOR_UNIT_DURATION",
    "YEAR",
]

COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_UI_TYPE_TYPE_1_VALUES: set[CompositeFilterFiltersItemType2UiTypeType1] = {
    "ANY_TIME",
    "BEFORE",
    "BETWEEN",
    "CUSTOM",
    "DAY",
    "IS_IN_THE_FISCAL_QUARTER",
    "IS_IN_THE_FISCAL_YEAR",
    "IS_IN_THE_MONTH",
    "IS_IN_THE_QUARTER",
    "IS_ON_DAY_OF_WEEK",
    "MONTH_OF_YEAR",
    "ON_OR_AFTER",
    "PAST",
    "TIME_FOR_INTERVAL_DURATION",
    "TIME_FOR_UNIT_DURATION",
    "YEAR",
}


def check_composite_filter_filters_item_type_2_ui_type_type_1(value: str) -> CompositeFilterFiltersItemType2UiTypeType1:
    if value in COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_UI_TYPE_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_FILTERS_ITEM_TYPE_2_UI_TYPE_TYPE_1_VALUES!r}"
    )
