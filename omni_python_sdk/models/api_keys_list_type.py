from typing import Literal

ApiKeysListType = Literal["mcp", "organization", "personal"]

API_KEYS_LIST_TYPE_VALUES: set[ApiKeysListType] = {
    "mcp",
    "organization",
    "personal",
}


def check_api_keys_list_type(value: str) -> ApiKeysListType:
    if value in API_KEYS_LIST_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {API_KEYS_LIST_TYPE_VALUES!r}")
