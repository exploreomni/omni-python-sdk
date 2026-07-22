from typing import Literal

UserAttributesListResponseRecordsItemType = Literal["Number", "String"]

USER_ATTRIBUTES_LIST_RESPONSE_RECORDS_ITEM_TYPE_VALUES: set[UserAttributesListResponseRecordsItemType] = {
    "Number",
    "String",
}


def check_user_attributes_list_response_records_item_type(value: str) -> UserAttributesListResponseRecordsItemType:
    if value in USER_ATTRIBUTES_LIST_RESPONSE_RECORDS_ITEM_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {USER_ATTRIBUTES_LIST_RESPONSE_RECORDS_ITEM_TYPE_VALUES!r}"
    )
