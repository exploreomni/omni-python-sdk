from typing import Literal

ApiKeyType = Literal["mcp", "organization", "personal"]

API_KEY_TYPE_VALUES: set[ApiKeyType] = {
    "mcp",
    "organization",
    "personal",
}


def check_api_key_type(value: str) -> ApiKeyType:
    if value in API_KEY_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {API_KEY_TYPE_VALUES!r}")
