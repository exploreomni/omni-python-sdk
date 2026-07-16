from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..models.routine_email_destination_type import RoutineEmailDestinationType, check_routine_email_destination_type
from ..types import UNSET, Unset

T = TypeVar("T", bound="RoutineEmailDestination")


@_attrs_define
class RoutineEmailDestination:
    """
    Attributes:
        type_ (RoutineEmailDestinationType): Selects email delivery — each scheduled run is sent to the listed email
            recipients and user groups. Example: email.
        recipient_emails (list[str] | Unset): Email addresses that receive each scheduled run of the routine. Example:
            ['alice@example.com', 'bob@example.com'].
        user_group_ids (list[UUID] | Unset): User group IDs whose active members receive each scheduled run. Omni
            expands each group to the members' current email addresses when the routine runs. Example:
            ['550e8400-e29b-41d4-a716-446655440000'].
    """

    type_: RoutineEmailDestinationType
    recipient_emails: list[str] | Unset = UNSET
    user_group_ids: list[UUID] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        recipient_emails: list[str] | Unset = UNSET
        if not isinstance(self.recipient_emails, Unset):
            recipient_emails = self.recipient_emails

        user_group_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_group_ids, Unset):
            user_group_ids = []
            for user_group_ids_item_data in self.user_group_ids:
                user_group_ids_item = str(user_group_ids_item_data)
                user_group_ids.append(user_group_ids_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if recipient_emails is not UNSET:
            field_dict["recipientEmails"] = recipient_emails
        if user_group_ids is not UNSET:
            field_dict["userGroupIds"] = user_group_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = check_routine_email_destination_type(d.pop("type"))

        recipient_emails = cast(list[str], d.pop("recipientEmails", UNSET))

        _user_group_ids = d.pop("userGroupIds", UNSET)
        user_group_ids: list[UUID] | Unset = UNSET
        if _user_group_ids is not UNSET:
            user_group_ids = []
            for user_group_ids_item_data in _user_group_ids:
                user_group_ids_item = UUID(user_group_ids_item_data)

                user_group_ids.append(user_group_ids_item)

        routine_email_destination = cls(
            type_=type_,
            recipient_emails=recipient_emails,
            user_group_ids=user_group_ids,
        )

        return routine_email_destination
