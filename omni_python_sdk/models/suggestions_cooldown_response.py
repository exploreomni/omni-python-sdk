from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SuggestionsCooldownResponse")


@_attrs_define
class SuggestionsCooldownResponse:
    """
    Attributes:
        detail (str): Human-readable error message.
        last_completed_at (datetime.datetime): When the most recent run completed.
        retry_after_seconds (int): Seconds to wait before a new run is allowed.
        status (int):  Example: 429.
    """

    detail: str
    last_completed_at: datetime.datetime
    retry_after_seconds: int
    status: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        detail = self.detail

        last_completed_at = self.last_completed_at.isoformat()

        retry_after_seconds = self.retry_after_seconds

        status = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "detail": detail,
                "lastCompletedAt": last_completed_at,
                "retryAfterSeconds": retry_after_seconds,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        detail = d.pop("detail")

        last_completed_at = datetime.datetime.fromisoformat(d.pop("lastCompletedAt"))

        retry_after_seconds = d.pop("retryAfterSeconds")

        status = d.pop("status")

        suggestions_cooldown_response = cls(
            detail=detail,
            last_completed_at=last_completed_at,
            retry_after_seconds=retry_after_seconds,
            status=status,
        )

        suggestions_cooldown_response.additional_properties = d
        return suggestions_cooldown_response

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
