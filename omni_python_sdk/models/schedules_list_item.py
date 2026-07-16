from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.schedules_list_item_alert import SchedulesListItemAlert


T = TypeVar("T", bound="SchedulesListItem")


@_attrs_define
class SchedulesListItem:
    """
    Attributes:
        content (str): Content type: dashboard or tile Example: dashboard.
        dashboard_name (str): Name of the dashboard Example: Weekly Sales Report.
        destination_type (str): Delivery destination type: email, slack, webhook, sftp, s3, google_sheets Example:
            email.
        disabled_at (datetime.datetime | None): Timestamp when the schedule was paused (null if active)
        format_ (str): Output format: pdf, png, csv, xlsx, json, link_only Example: pdf.
        id (UUID): Unique identifier for the schedule
        identifier (str): Dashboard identifier Example: 12db1a0a.
        last_completed_at (datetime.datetime | None): Timestamp of last completed delivery
        last_status (None | str): Status of last delivery: COMPLETE, ERROR, ERROR_DELIVERED, KILLED, CONDITION_UNMET
        name (str): Name of the schedule Example: Weekly Sales Report.
        owner_id (UUID): User ID of the schedule owner
        owner_name (str): Display name of the schedule owner Example: John Doe.
        recipient_count (float): Number of recipients (-1 for non-email destinations) Example: 5.
        schedule (str): AWS EventBridge cron expression (minute hour day-of-month month day-of-week year) Example: 0 9 ?
            * MON *.
        slack_recipient_type (None | str): Slack recipient type: Channel or Users (null for non-Slack)
        system_disabled_at (datetime.datetime | None): Timestamp when system disabled the schedule (null if not system-
            disabled)
        system_disabled_reason (None | str): Reason for system disabling: missingQuery, noAccess,
            orphanedFilterConfigKeys
        timezone (str): IANA timezone for the schedule Example: America/New_York.
        alert (SchedulesListItemAlert | Unset): Alert configuration (only present for alert-type schedules)
    """

    content: str
    dashboard_name: str
    destination_type: str
    disabled_at: datetime.datetime | None
    format_: str
    id: UUID
    identifier: str
    last_completed_at: datetime.datetime | None
    last_status: None | str
    name: str
    owner_id: UUID
    owner_name: str
    recipient_count: float
    schedule: str
    slack_recipient_type: None | str
    system_disabled_at: datetime.datetime | None
    system_disabled_reason: None | str
    timezone: str
    alert: SchedulesListItemAlert | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        content = self.content

        dashboard_name = self.dashboard_name

        destination_type = self.destination_type

        disabled_at: None | str
        if isinstance(self.disabled_at, datetime.datetime):
            disabled_at = self.disabled_at.isoformat()
        else:
            disabled_at = self.disabled_at

        format_ = self.format_

        id = str(self.id)

        identifier = self.identifier

        last_completed_at: None | str
        if isinstance(self.last_completed_at, datetime.datetime):
            last_completed_at = self.last_completed_at.isoformat()
        else:
            last_completed_at = self.last_completed_at

        last_status: None | str
        last_status = self.last_status

        name = self.name

        owner_id = str(self.owner_id)

        owner_name = self.owner_name

        recipient_count = self.recipient_count

        schedule = self.schedule

        slack_recipient_type: None | str
        slack_recipient_type = self.slack_recipient_type

        system_disabled_at: None | str
        if isinstance(self.system_disabled_at, datetime.datetime):
            system_disabled_at = self.system_disabled_at.isoformat()
        else:
            system_disabled_at = self.system_disabled_at

        system_disabled_reason: None | str
        system_disabled_reason = self.system_disabled_reason

        timezone = self.timezone

        alert: dict[str, Any] | Unset = UNSET
        if not isinstance(self.alert, Unset):
            alert = self.alert.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "content": content,
                "dashboardName": dashboard_name,
                "destinationType": destination_type,
                "disabledAt": disabled_at,
                "format": format_,
                "id": id,
                "identifier": identifier,
                "lastCompletedAt": last_completed_at,
                "lastStatus": last_status,
                "name": name,
                "ownerId": owner_id,
                "ownerName": owner_name,
                "recipientCount": recipient_count,
                "schedule": schedule,
                "slackRecipientType": slack_recipient_type,
                "systemDisabledAt": system_disabled_at,
                "systemDisabledReason": system_disabled_reason,
                "timezone": timezone,
            }
        )
        if alert is not UNSET:
            field_dict["alert"] = alert

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.schedules_list_item_alert import SchedulesListItemAlert

        d = dict(src_dict)
        content = d.pop("content")

        dashboard_name = d.pop("dashboardName")

        destination_type = d.pop("destinationType")

        def _parse_disabled_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                disabled_at_type_0 = datetime.datetime.fromisoformat(data)

                return disabled_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        disabled_at = _parse_disabled_at(d.pop("disabledAt"))

        format_ = d.pop("format")

        id = UUID(d.pop("id"))

        identifier = d.pop("identifier")

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

        name = d.pop("name")

        owner_id = UUID(d.pop("ownerId"))

        owner_name = d.pop("ownerName")

        recipient_count = d.pop("recipientCount")

        schedule = d.pop("schedule")

        def _parse_slack_recipient_type(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        slack_recipient_type = _parse_slack_recipient_type(d.pop("slackRecipientType"))

        def _parse_system_disabled_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                system_disabled_at_type_0 = datetime.datetime.fromisoformat(data)

                return system_disabled_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        system_disabled_at = _parse_system_disabled_at(d.pop("systemDisabledAt"))

        def _parse_system_disabled_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        system_disabled_reason = _parse_system_disabled_reason(d.pop("systemDisabledReason"))

        timezone = d.pop("timezone")

        _alert = d.pop("alert", UNSET)
        alert: SchedulesListItemAlert | Unset
        if isinstance(_alert, Unset):
            alert = UNSET
        else:
            alert = SchedulesListItemAlert.from_dict(_alert)

        schedules_list_item = cls(
            content=content,
            dashboard_name=dashboard_name,
            destination_type=destination_type,
            disabled_at=disabled_at,
            format_=format_,
            id=id,
            identifier=identifier,
            last_completed_at=last_completed_at,
            last_status=last_status,
            name=name,
            owner_id=owner_id,
            owner_name=owner_name,
            recipient_count=recipient_count,
            schedule=schedule,
            slack_recipient_type=slack_recipient_type,
            system_disabled_at=system_disabled_at,
            system_disabled_reason=system_disabled_reason,
            timezone=timezone,
            alert=alert,
        )

        schedules_list_item.additional_properties = d
        return schedules_list_item

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
