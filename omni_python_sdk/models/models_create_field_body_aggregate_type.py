from typing import Literal

ModelsCreateFieldBodyAggregateType = Literal[
    "AVERAGE",
    "AVERAGE_DISTINCT_ON",
    "COUNT",
    "COUNT_DISTINCT",
    "LIST",
    "MAX",
    "MEDIAN",
    "MEDIAN_DISTINCT_ON",
    "MIN",
    "PERCENTILE",
    "PERCENTILE_DISTINCT_ON",
    "SEMANTIC_VIEW_AGG",
    "SUM",
    "SUM_DISTINCT_ON",
]

MODELS_CREATE_FIELD_BODY_AGGREGATE_TYPE_VALUES: set[ModelsCreateFieldBodyAggregateType] = {
    "AVERAGE",
    "AVERAGE_DISTINCT_ON",
    "COUNT",
    "COUNT_DISTINCT",
    "LIST",
    "MAX",
    "MEDIAN",
    "MEDIAN_DISTINCT_ON",
    "MIN",
    "PERCENTILE",
    "PERCENTILE_DISTINCT_ON",
    "SEMANTIC_VIEW_AGG",
    "SUM",
    "SUM_DISTINCT_ON",
}


def check_models_create_field_body_aggregate_type(value: str) -> ModelsCreateFieldBodyAggregateType:
    if value in MODELS_CREATE_FIELD_BODY_AGGREGATE_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_CREATE_FIELD_BODY_AGGREGATE_TYPE_VALUES!r}")
