from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SchedulesAddRecipientsBody")


@_attrs_define
class SchedulesAddRecipientsBody:
    """
    Attributes:
        emails (list[str] | Unset): At least one email, userId, or userGroupId must be provided. Array of email
            addresses to add as recipients. Example: ['user@example.com'].
        user_group_ids (list[UUID] | Unset): At least one email, userId, or userGroupId must be provided. Array of user
            group UUIDs to add as recipients. Example: ['123e4567-e89b-12d3-a456-426614174000'].
        user_ids (list[UUID] | Unset): At least one email, userId, or userGroupId must be provided. Array of user UUIDs
            to add as recipients. Use the List users and List embed users endpoints to retrieve user IDs. Example:
            ['987fcdeb-51a2-43d7-9b56-254415f67890'].
    """

    emails: list[str] | Unset = UNSET
    user_group_ids: list[UUID] | Unset = UNSET
    user_ids: list[UUID] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        emails: list[str] | Unset = UNSET
        if not isinstance(self.emails, Unset):
            emails = self.emails

        user_group_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_group_ids, Unset):
            user_group_ids = []
            for user_group_ids_item_data in self.user_group_ids:
                user_group_ids_item = str(user_group_ids_item_data)
                user_group_ids.append(user_group_ids_item)

        user_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_ids, Unset):
            user_ids = []
            for user_ids_item_data in self.user_ids:
                user_ids_item = str(user_ids_item_data)
                user_ids.append(user_ids_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if emails is not UNSET:
            field_dict["emails"] = emails
        if user_group_ids is not UNSET:
            field_dict["userGroupIds"] = user_group_ids
        if user_ids is not UNSET:
            field_dict["userIds"] = user_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        emails = cast(list[str], d.pop("emails", UNSET))

        _user_group_ids = d.pop("userGroupIds", UNSET)
        user_group_ids: list[UUID] | Unset = UNSET
        if _user_group_ids is not UNSET:
            user_group_ids = []
            for user_group_ids_item_data in _user_group_ids:
                user_group_ids_item = UUID(user_group_ids_item_data)

                user_group_ids.append(user_group_ids_item)

        _user_ids = d.pop("userIds", UNSET)
        user_ids: list[UUID] | Unset = UNSET
        if _user_ids is not UNSET:
            user_ids = []
            for user_ids_item_data in _user_ids:
                user_ids_item = UUID(user_ids_item_data)

                user_ids.append(user_ids_item)

        schedules_add_recipients_body = cls(
            emails=emails,
            user_group_ids=user_group_ids,
            user_ids=user_ids,
        )

        schedules_add_recipients_body.additional_properties = d
        return schedules_add_recipients_body

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
