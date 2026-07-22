from typing import Literal

ContentListResponseRecordsItemType1Scope = Literal["organization", "restricted"]

CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_SCOPE_VALUES: set[ContentListResponseRecordsItemType1Scope] = {
    "organization",
    "restricted",
}


def check_content_list_response_records_item_type_1_scope(value: str) -> ContentListResponseRecordsItemType1Scope:
    if value in CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_SCOPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_SCOPE_VALUES!r}"
    )
