from typing import Literal

CompositeFilterConjunction = Literal["AND", "OR"]

COMPOSITE_FILTER_CONJUNCTION_VALUES: set[CompositeFilterConjunction] = {
    "AND",
    "OR",
}


def check_composite_filter_conjunction(value: str) -> CompositeFilterConjunction:
    if value in COMPOSITE_FILTER_CONJUNCTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {COMPOSITE_FILTER_CONJUNCTION_VALUES!r}")
