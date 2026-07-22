from typing import Literal

QueryRunBodyResultType = Literal["csv", "json", "xlsx"]

QUERY_RUN_BODY_RESULT_TYPE_VALUES: set[QueryRunBodyResultType] = {
    "csv",
    "json",
    "xlsx",
}


def check_query_run_body_result_type(value: str) -> QueryRunBodyResultType:
    if value in QUERY_RUN_BODY_RESULT_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {QUERY_RUN_BODY_RESULT_TYPE_VALUES!r}")
