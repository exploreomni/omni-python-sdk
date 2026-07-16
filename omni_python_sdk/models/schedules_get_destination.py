from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.schedules_get_recipient import SchedulesGetRecipient


T = TypeVar("T", bound="SchedulesGetDestination")


@_attrs_define
class SchedulesGetDestination:
    """
    Attributes:
        format_ (str): Output format: pdf, png, csv, xlsx, json, link_only Example: pdf.
        id (UUID): Destination UUID
        last_completed_at (datetime.datetime | None): Timestamp of last completed delivery
        last_status (None | str): Status of last delivery: COMPLETE, ERROR, ERROR_DELIVERED, KILLED, CONDITION_UNMET
        recipients (list[SchedulesGetRecipient]): Individual email recipients
        user_group_recipients (list[Any]): User group recipients
        metadata (Any | Unset): Destination-specific configuration (type, recipients, credentials, etc.)
    """

    format_: str
    id: UUID
    last_completed_at: datetime.datetime | None
    last_status: None | str
    recipients: list[SchedulesGetRecipient]
    user_group_recipients: list[Any]
    metadata: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        format_ = self.format_

        id = str(self.id)

        last_completed_at: None | str
        if isinstance(self.last_completed_at, datetime.datetime):
            last_completed_at = self.last_completed_at.isoformat()
        else:
            last_completed_at = self.last_completed_at

        last_status: None | str
        last_status = self.last_status

        recipients = []
        for recipients_item_data in self.recipients:
            recipients_item = recipients_item_data.to_dict()
            recipients.append(recipients_item)

        user_group_recipients = self.user_group_recipients

        metadata = self.metadata

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "format": format_,
                "id": id,
                "lastCompletedAt": last_completed_at,
                "lastStatus": last_status,
                "recipients": recipients,
                "userGroupRecipients": user_group_recipients,
            }
        )
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.schedules_get_recipient import SchedulesGetRecipient

        d = dict(src_dict)
        format_ = d.pop("format")

        id = UUID(d.pop("id"))

        def _parse_last_completed_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                last_completed_at_type_0 = datetime.datetime.fromisoformat(data)

                return last_completed_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        last_completed_at = _parse_last_completed_at(d.pop("lastCompletedAt"))

        def _parse_last_status(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        last_status = _parse_last_status(d.pop("lastStatus"))

        recipients = []
        _recipients = d.pop("recipients")
        for recipients_item_data in _recipients:
            recipients_item = SchedulesGetRecipient.from_dict(recipients_item_data)

            recipients.append(recipients_item)

        user_group_recipients = cast(list[Any], d.pop("userGroupRecipients"))

        metadata = d.pop("metadata", UNSET)

        schedules_get_destination = cls(
            format_=format_,
            id=id,
            last_completed_at=last_completed_at,
            last_status=last_status,
            recipients=recipients,
            user_group_recipients=user_group_recipients,
            metadata=metadata,
        )

        schedules_get_destination.additional_properties = d
        return schedules_get_destination

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
