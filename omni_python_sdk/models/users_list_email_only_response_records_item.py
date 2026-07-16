from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.users_list_email_only_response_records_item_user_attributes import (
        UsersListEmailOnlyResponseRecordsItemUserAttributes,
    )


T = TypeVar("T", bound="UsersListEmailOnlyResponseRecordsItem")


@_attrs_define
class UsersListEmailOnlyResponseRecordsItem:
    """
    Attributes:
        email (str): User email address Example: user@example.com.
        user_attributes (UsersListEmailOnlyResponseRecordsItemUserAttributes): User attributes as key-value pairs
        user_id (UUID): User ID
    """

    email: str
    user_attributes: UsersListEmailOnlyResponseRecordsItemUserAttributes
    user_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        email = self.email

        user_attributes = self.user_attributes.to_dict()

        user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "email": email,
                "user_attributes": user_attributes,
                "user_id": user_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.users_list_email_only_response_records_item_user_attributes import (
            UsersListEmailOnlyResponseRecordsItemUserAttributes,
        )

        d = dict(src_dict)
        email = d.pop("email")

        user_attributes = UsersListEmailOnlyResponseRecordsItemUserAttributes.from_dict(d.pop("user_attributes"))

        user_id = UUID(d.pop("user_id"))

        users_list_email_only_response_records_item = cls(
            email=email,
            user_attributes=user_attributes,
            user_id=user_id,
        )

        users_list_email_only_response_records_item.additional_properties = d
        return users_list_email_only_response_records_item

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
