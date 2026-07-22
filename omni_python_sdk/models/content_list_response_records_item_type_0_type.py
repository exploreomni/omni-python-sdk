from typing import Literal

ContentListResponseRecordsItemType0Type = Literal["document"]

CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_0_TYPE_VALUES: set[ContentListResponseRecordsItemType0Type] = {
    "document",
}


def check_content_list_response_records_item_type_0_type(value: str) -> ContentListResponseRecordsItemType0Type:
    if value in CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_0_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_RESPONSE_RECORDS_ITEM_TYPE_0_TYPE_VALUES!r}"
    )
