from typing import Literal

CompositeFilterType = Literal["composite"]

COMPOSITE_FILTER_TYPE_VALUES: set[CompositeFilterType] = {
    "composite",
}


def check_composite_filter_type(value: str) -> CompositeFilterType:
    if value in COMPOSITE_FILTER_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_TYPE_VALUES!r}")
