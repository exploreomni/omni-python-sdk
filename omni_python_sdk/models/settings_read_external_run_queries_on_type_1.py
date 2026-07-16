from typing import Literal

SettingsReadExternalRunQueriesOnType1 = Literal["all-pages", "current-page"]

SETTINGS_READ_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES: set[SettingsReadExternalRunQueriesOnType1] = {
    "all-pages",
    "current-page",
}


def check_settings_read_external_run_queries_on_type_1(value: str) -> SettingsReadExternalRunQueriesOnType1:
    if value in SETTINGS_READ_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SETTINGS_READ_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES!r}"
    )
