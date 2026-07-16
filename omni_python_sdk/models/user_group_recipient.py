from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.email_recipient import EmailRecipient


T = TypeVar("T", bound="UserGroupRecipient")


@_attrs_define
class UserGroupRecipient:
    """
    Attributes:
        id (str): User group ID.
        name (str): User group name. Example: Sales Team.
        recipients (list[EmailRecipient]): List of recipients in the user group.
    """

    id: str
    name: str
    recipients: list[EmailRecipient]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        recipients = []
        for recipients_item_data in self.recipients:
            recipients_item = recipients_item_data.to_dict()
            recipients.append(recipients_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "recipients": recipients,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.email_recipient import EmailRecipient

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        recipients = []
        _recipients = d.pop("recipients")
        for recipients_item_data in _recipients:
            recipients_item = EmailRecipient.from_dict(recipients_item_data)

            recipients.append(recipients_item)

        user_group_recipient = cls(
            id=id,
            name=name,
            recipients=recipients,
        )

        user_group_recipient.additional_properties = d
        return user_group_recipient

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
