from typing import Literal

SettingsPatchExternalRunQueriesOnType1 = Literal["all-pages", "current-page"]

SETTINGS_PATCH_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES: set[SettingsPatchExternalRunQueriesOnType1] = {
    "all-pages",
    "current-page",
}


def check_settings_patch_external_run_queries_on_type_1(value: str) -> SettingsPatchExternalRunQueriesOnType1:
    if value in SETTINGS_PATCH_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SETTINGS_PATCH_EXTERNAL_RUN_QUERIES_ON_TYPE_1_VALUES!r}"
    )
