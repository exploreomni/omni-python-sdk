from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..models.routine_email_destination_response_type import (
    RoutineEmailDestinationResponseType,
    check_routine_email_destination_response_type,
)

T = TypeVar("T", bound="RoutineEmailDestinationResponse")


@_attrs_define
class RoutineEmailDestinationResponse:
    """
    Attributes:
        recipient_emails (list[str]): Email addresses configured as direct recipients of each scheduled run, resolved
            from their current membership. Example: ['alice@example.com', 'bob@example.com'].
        type_ (RoutineEmailDestinationResponseType): Selects email delivery — each scheduled run is sent to the listed
            email recipients and user groups. Example: email.
        user_group_ids (list[UUID]): User group IDs whose active members receive each scheduled run. Omni expands each
            group to the members' current email addresses when the routine runs. Example:
            ['550e8400-e29b-41d4-a716-446655440000'].
    """

    recipient_emails: list[str]
    type_: RoutineEmailDestinationResponseType
    user_group_ids: list[UUID]

    def to_dict(self) -> dict[str, Any]:
        recipient_emails = self.recipient_emails

        type_: str = self.type_

        user_group_ids = []
        for user_group_ids_item_data in self.user_group_ids:
            user_group_ids_item = str(user_group_ids_item_data)
            user_group_ids.append(user_group_ids_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "recipientEmails": recipient_emails,
                "type": type_,
                "userGroupIds": user_group_ids,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        recipient_emails = cast(list[str], d.pop("recipientEmails"))

        type_ = check_routine_email_destination_response_type(d.pop("type"))

        user_group_ids = []
        _user_group_ids = d.pop("userGroupIds")
        for user_group_ids_item_data in _user_group_ids:
            user_group_ids_item = UUID(user_group_ids_item_data)

            user_group_ids.append(user_group_ids_item)

        routine_email_destination_response = cls(
            recipient_emails=recipient_emails,
            type_=type_,
            user_group_ids=user_group_ids,
        )

        return routine_email_destination_response
