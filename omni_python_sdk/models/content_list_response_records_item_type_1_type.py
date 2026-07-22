from typing import Literal

ContentListResponseRecordsItemType1Type = Literal["folder"]

CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_TYPE_VALUES: set[ContentListResponseRecordsItemType1Type] = {
    "folder",
}


def check_content_list_response_records_item_type_1_type(value: str) -> ContentListResponseRecordsItemType1Type:
    if value in CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_1_TYPE_VALUES!r}"
    )
