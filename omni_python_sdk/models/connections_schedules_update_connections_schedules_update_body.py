from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectionsSchedulesUpdateConnectionsSchedulesUpdateBody")


@_attrs_define
class ConnectionsSchedulesUpdateConnectionsSchedulesUpdateBody:
    """Request body for updating a schema refresh schedule

    Attributes:
        schedule (str): AWS EventBridge cron expression (6 fields: minute hour day-of-month month day-of-week year). See
            https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-cron-expressions.html Example: 0 2 * * ? *.
        timezone (str): IANA timezone for schedule execution Example: America/New_York.
        hard_refresh (bool | Unset): When true, the scheduled refresh performs a hard refresh that fully discards and
            rebuilds the schema model. When false (the default), it performs a soft refresh that merges newly generated
            views with the existing model. Default: False.
    """

    schedule: str
    timezone: str
    hard_refresh: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        schedule = self.schedule

        timezone = self.timezone

        hard_refresh = self.hard_refresh

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "schedule": schedule,
                "timezone": timezone,
            }
        )
        if hard_refresh is not UNSET:
            field_dict["hardRefresh"] = hard_refresh

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        schedule = d.pop("schedule")

        timezone = d.pop("timezone")

        hard_refresh = d.pop("hardRefresh", UNSET)

        connections_schedules_update_connections_schedules_update_body = cls(
            schedule=schedule,
            timezone=timezone,
            hard_refresh=hard_refresh,
        )

        connections_schedules_update_connections_schedules_update_body.additional_properties = d
        return connections_schedules_update_connections_schedules_update_body

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
