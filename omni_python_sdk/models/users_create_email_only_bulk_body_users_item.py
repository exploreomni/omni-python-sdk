from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.users_create_email_only_bulk_body_users_item_user_attributes import (
        UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes,
    )


T = TypeVar("T", bound="UsersCreateEmailOnlyBulkBodyUsersItem")


@_attrs_define
class UsersCreateEmailOnlyBulkBodyUsersItem:
    """
    Attributes:
        email (str): Email address for the user Example: user@example.com.
        user_attributes (UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes | Unset): Optional user attributes as key-
            value pairs
    """

    email: str
    user_attributes: UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        email = self.email

        user_attributes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.user_attributes, Unset):
            user_attributes = self.user_attributes.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "email": email,
            }
        )
        if user_attributes is not UNSET:
            field_dict["userAttributes"] = user_attributes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.users_create_email_only_bulk_body_users_item_user_attributes import (
            UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes,
        )

        d = dict(src_dict)
        email = d.pop("email")

        _user_attributes = d.pop("userAttributes", UNSET)
        user_attributes: UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes | Unset
        if isinstance(_user_attributes, Unset):
            user_attributes = UNSET
        else:
            user_attributes = UsersCreateEmailOnlyBulkBodyUsersItemUserAttributes.from_dict(_user_attributes)

        users_create_email_only_bulk_body_users_item = cls(
            email=email,
            user_attributes=user_attributes,
        )

        users_create_email_only_bulk_body_users_item.additional_properties = d
        return users_create_email_only_bulk_body_users_item

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
