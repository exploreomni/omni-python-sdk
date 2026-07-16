from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiJobStatusResponseProgressType0")


@_attrs_define
class AiJobStatusResponseProgressType0:
    """Real-time progress information. Only present in EXECUTING state. Null if no progress has been reported yet. Updated
    in real-time as the AI works through iterations.

        Attributes:
            iteration (int): Current iteration number. The AI may take multiple iterations to refine queries and generate a
                complete answer. Example: 2.
            message (str): Human-readable status message describing what the AI is currently doing. Example: Running query:
                Top products by revenue.
            updated_at (datetime.datetime): When this progress update was recorded. Example: 2025-01-15T10:00:08.000Z.
    """

    iteration: int
    message: str
    updated_at: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        iteration = self.iteration

        message = self.message

        updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "iteration": iteration,
                "message": message,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        iteration = d.pop("iteration")

        message = d.pop("message")

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        ai_job_status_response_progress_type_0 = cls(
            iteration=iteration,
            message=message,
            updated_at=updated_at,
        )

        ai_job_status_response_progress_type_0.additional_properties = d
        return ai_job_status_response_progress_type_0

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
