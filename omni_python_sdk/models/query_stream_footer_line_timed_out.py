from typing import Literal

QueryStreamFooterLineTimedOut = Literal["false", "true"]

QUERY_STREAM_FOOTER_LINE_TIMED_OUT_VALUES: set[QueryStreamFooterLineTimedOut] = {
    "false",
    "true",
}


def check_query_stream_footer_line_timed_out(value: str) -> QueryStreamFooterLineTimedOut:
    if value in QUERY_STREAM_FOOTER_LINE_TIMED_OUT_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {QUERY_STREAM_FOOTER_LINE_TIMED_OUT_VALUES!r}")
