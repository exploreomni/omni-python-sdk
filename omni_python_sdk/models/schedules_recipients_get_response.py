from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.schedules_recipients_get_response_type import (
    SchedulesRecipientsGetResponseType,
    check_schedules_recipients_get_response_type,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.email_recipient import EmailRecipient
    from ..models.user_group_recipient import UserGroupRecipient


T = TypeVar("T", bound="SchedulesRecipientsGetResponse")


@_attrs_define
class SchedulesRecipientsGetResponse:
    """
    Attributes:
        type_ (SchedulesRecipientsGetResponseType): The schedule's destination type. Example: email.
        recipients (list[EmailRecipient] | Unset): List of individual recipients (for email destinations).
        user_group_recipients (list[UserGroupRecipient] | Unset): List of user group recipients (for email
            destinations).
    """

    type_: SchedulesRecipientsGetResponseType
    recipients: list[EmailRecipient] | Unset = UNSET
    user_group_recipients: list[UserGroupRecipient] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        recipients: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.recipients, Unset):
            recipients = []
            for recipients_item_data in self.recipients:
                recipients_item = recipients_item_data.to_dict()
                recipients.append(recipients_item)

        user_group_recipients: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.user_group_recipients, Unset):
            user_group_recipients = []
            for user_group_recipients_item_data in self.user_group_recipients:
                user_group_recipients_item = user_group_recipients_item_data.to_dict()
                user_group_recipients.append(user_group_recipients_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if recipients is not UNSET:
            field_dict["recipients"] = recipients
        if user_group_recipients is not UNSET:
            field_dict["userGroupRecipients"] = user_group_recipients

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.email_recipient import EmailRecipient
        from ..models.user_group_recipient import UserGroupRecipient

        d = dict(src_dict)
        type_ = check_schedules_recipients_get_response_type(d.pop("type"))

        _recipients = d.pop("recipients", UNSET)
        recipients: list[EmailRecipient] | Unset = UNSET
        if _recipients is not UNSET:
            recipients = []
            for recipients_item_data in _recipients:
                recipients_item = EmailRecipient.from_dict(recipients_item_data)

                recipients.append(recipients_item)

        _user_group_recipients = d.pop("userGroupRecipients", UNSET)
        user_group_recipients: list[UserGroupRecipient] | Unset = UNSET
        if _user_group_recipients is not UNSET:
            user_group_recipients = []
            for user_group_recipients_item_data in _user_group_recipients:
                user_group_recipients_item = UserGroupRecipient.from_dict(user_group_recipients_item_data)

                user_group_recipients.append(user_group_recipients_item)

        schedules_recipients_get_response = cls(
            type_=type_,
            recipients=recipients,
            user_group_recipients=user_group_recipients,
        )

        schedules_recipients_get_response.additional_properties = d
        return schedules_recipients_get_response

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
