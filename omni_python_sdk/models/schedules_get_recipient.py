from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.schedules_get_recipient_membership import SchedulesGetRecipientMembership


T = TypeVar("T", bound="SchedulesGetRecipient")


@_attrs_define
class SchedulesGetRecipient:
    """
    Attributes:
        id (UUID): Recipient ID
        membership (SchedulesGetRecipientMembership):
        membership_id (UUID): Membership ID
    """

    id: UUID
    membership: SchedulesGetRecipientMembership
    membership_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        membership = self.membership.to_dict()

        membership_id = str(self.membership_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "membership": membership,
                "membershipId": membership_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.schedules_get_recipient_membership import SchedulesGetRecipientMembership

        d = dict(src_dict)
        id = UUID(d.pop("id"))

        membership = SchedulesGetRecipientMembership.from_dict(d.pop("membership"))

        membership_id = UUID(d.pop("membershipId"))

        schedules_get_recipient = cls(
            id=id,
            membership=membership,
            membership_id=membership_id,
        )

        schedules_get_recipient.additional_properties = d
        return schedules_get_recipient

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
