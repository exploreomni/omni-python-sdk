from typing import Literal

ModelsValidateResponseIssuesItemSeverity = Literal["error", "warning"]

MODELS_VALIDATE_RESPONSE_ISSUES_ITEM_SEVERITY_VALUES: set[ModelsValidateResponseIssuesItemSeverity] = {
    "error",
    "warning",
}


def check_models_validate_response_issues_item_severity(value: str) -> ModelsValidateResponseIssuesItemSeverity:
    if value in MODELS_VALIDATE_RESPONSE_ISSUES_ITEM_SEVERITY_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_VALIDATE_RESPONSE_ISSUES_ITEM_SEVERITY_VALUES!r}"
    )
