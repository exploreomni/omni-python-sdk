from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.schedule_suggestions_response_status import (
    ScheduleSuggestionsResponseStatus,
    check_schedule_suggestions_response_status,
)

T = TypeVar("T", bound="ScheduleSuggestionsResponse")


@_attrs_define
class ScheduleSuggestionsResponse:
    """
    Attributes:
        id (UUID): The schedule (trigger) id.
        shared_model_id (UUID): The shared model the schedule generates suggestions for.
        status (ScheduleSuggestionsResponseStatus):
        timezone (str): IANA timezone the schedule runs in. Example: America/New_York.
    """

    id: UUID
    shared_model_id: UUID
    status: ScheduleSuggestionsResponseStatus
    timezone: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        shared_model_id = str(self.shared_model_id)

        status: str = self.status

        timezone = self.timezone

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "sharedModelId": shared_model_id,
                "status": status,
                "timezone": timezone,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = UUID(d.pop("id"))

        shared_model_id = UUID(d.pop("sharedModelId"))

        status = check_schedule_suggestions_response_status(d.pop("status"))

        timezone = d.pop("timezone")

        schedule_suggestions_response = cls(
            id=id,
            shared_model_id=shared_model_id,
            status=status,
            timezone=timezone,
        )

        schedule_suggestions_response.additional_properties = d
        return schedule_suggestions_response

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
