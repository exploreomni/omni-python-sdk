from typing import Literal

UsersListEmailOnlySortDirection = Literal["asc", "desc"]

USERS_LIST_EMAIL_ONLY_SORT_DIRECTION_VALUES: set[UsersListEmailOnlySortDirection] = {
    "asc",
    "desc",
}


def check_users_list_email_only_sort_direction(value: str) -> UsersListEmailOnlySortDirection:
    if value in USERS_LIST_EMAIL_ONLY_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {USERS_LIST_EMAIL_ONLY_SORT_DIRECTION_VALUES!r}")
