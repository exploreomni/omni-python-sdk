from typing import Literal

QueryRunBodyCache = Literal["disabled", "normal", "refresh", "refresh_all"]

QUERY_RUN_BODY_CACHE_VALUES: set[QueryRunBodyCache] = {
    "disabled",
    "normal",
    "refresh",
    "refresh_all",
}


def check_query_run_body_cache(value: str) -> QueryRunBodyCache:
    if value in QUERY_RUN_BODY_CACHE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {QUERY_RUN_BODY_CACHE_VALUES!r}")
