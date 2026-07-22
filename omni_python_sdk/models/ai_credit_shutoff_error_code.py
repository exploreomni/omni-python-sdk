from typing import Literal

AiCreditShutoffErrorCode = Literal["shutoff"]

AI_CREDIT_SHUTOFF_ERROR_CODE_VALUES: set[AiCreditShutoffErrorCode] = {
    "shutoff",
}


def check_ai_credit_shutoff_error_code(value: str) -> AiCreditShutoffErrorCode:
    if value in AI_CREDIT_SHUTOFF_ERROR_CODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_CREDIT_SHUTOFF_ERROR_CODE_VALUES!r}")
