from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule")


@_attrs_define
class ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule:
    """Schema refresh schedule object

    Attributes:
        connection_id (UUID): Connection ID this schedule belongs to Example: 550e8400-e29b-41d4-a716-446655440000.
        created_at (str): Schedule creation timestamp (ISO 8601) Example: 2024-01-15T10:30:00Z.
        description (str): Human-readable schedule description Example: Runs daily at 2:00 AM EST.
        disabled_at (None | str): Timestamp when schedule was disabled (ISO 8601)
        hard_refresh (bool): When true, the scheduled refresh performs a hard refresh that fully discards and rebuilds
            the schema model. When false, it performs a soft refresh that merges newly generated views with the existing
            model.
        schedule (str): AWS EventBridge cron expression (6 fields: minute hour day-of-month month day-of-week year). See
            https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-cron-expressions.html Example: 0 2 * * ? *.
        schedule_id (UUID): Unique schedule identifier Example: 550e8400-e29b-41d4-a716-446655440001.
        timezone (str): IANA timezone for schedule execution Example: America/New_York.
        updated_at (str): Schedule last update timestamp (ISO 8601) Example: 2024-01-15T10:30:00Z.
    """

    connection_id: UUID
    created_at: str
    description: str
    disabled_at: None | str
    hard_refresh: bool
    schedule: str
    schedule_id: UUID
    timezone: str
    updated_at: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_id = str(self.connection_id)

        created_at = self.created_at

        description = self.description

        disabled_at: None | str
        disabled_at = self.disabled_at

        hard_refresh = self.hard_refresh

        schedule = self.schedule

        schedule_id = str(self.schedule_id)

        timezone = self.timezone

        updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionId": connection_id,
                "createdAt": created_at,
                "description": description,
                "disabledAt": disabled_at,
                "hardRefresh": hard_refresh,
                "schedule": schedule,
                "scheduleId": schedule_id,
                "timezone": timezone,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_id = UUID(d.pop("connectionId"))

        created_at = d.pop("createdAt")

        description = d.pop("description")

        def _parse_disabled_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        disabled_at = _parse_disabled_at(d.pop("disabledAt"))

        hard_refresh = d.pop("hardRefresh")

        schedule = d.pop("schedule")

        schedule_id = UUID(d.pop("scheduleId"))

        timezone = d.pop("timezone")

        updated_at = d.pop("updatedAt")

        connections_schedules_list_connections_schedules_list_response_connection_schedule = cls(
            connection_id=connection_id,
            created_at=created_at,
            description=description,
            disabled_at=disabled_at,
            hard_refresh=hard_refresh,
            schedule=schedule,
            schedule_id=schedule_id,
            timezone=timezone,
            updated_at=updated_at,
        )

        connections_schedules_list_connections_schedules_list_response_connection_schedule.additional_properties = d
        return connections_schedules_list_connections_schedules_list_response_connection_schedule

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
