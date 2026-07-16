from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.connections_schedules_list_connections_schedules_list_response_connection_schedule import (
        ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule,
    )


T = TypeVar("T", bound="ConnectionsSchedulesListConnectionsSchedulesListResponse")


@_attrs_define
class ConnectionsSchedulesListConnectionsSchedulesListResponse:
    """List schedules response

    Attributes:
        schedules (list[ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule]): List of schema
            refresh schedules
    """

    schedules: list[ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        schedules = []
        for schedules_item_data in self.schedules:
            schedules_item = schedules_item_data.to_dict()
            schedules.append(schedules_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "schedules": schedules,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connections_schedules_list_connections_schedules_list_response_connection_schedule import (
            ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule,
        )

        d = dict(src_dict)
        schedules = []
        _schedules = d.pop("schedules")
        for schedules_item_data in _schedules:
            schedules_item = ConnectionsSchedulesListConnectionsSchedulesListResponseConnectionSchedule.from_dict(
                schedules_item_data
            )

            schedules.append(schedules_item)

        connections_schedules_list_connections_schedules_list_response = cls(
            schedules=schedules,
        )

        connections_schedules_list_connections_schedules_list_response.additional_properties = d
        return connections_schedules_list_connections_schedules_list_response

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
