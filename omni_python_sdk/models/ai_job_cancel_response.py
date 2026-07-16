from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_job_cancel_response_state import AiJobCancelResponseState, check_ai_job_cancel_response_state

T = TypeVar("T", bound="AiJobCancelResponse")


@_attrs_define
class AiJobCancelResponse:
    """
    Attributes:
        job_id (UUID): The job ID that was requested to cancel. Example: 550e8400-e29b-41d4-a716-446655440000.
        state (AiJobCancelResponseState): The job state after the cancellation attempt. CANCELLED if the cancellation
            was successful. If the job was already in a terminal state (COMPLETE, FAILED, CANCELLED), the current state is
            returned unchanged — the endpoint is idempotent. Example: CANCELLED.
    """

    job_id: UUID
    state: AiJobCancelResponseState
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = str(self.job_id)

        state: str = self.state

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "jobId": job_id,
                "state": state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        job_id = UUID(d.pop("jobId"))

        state = check_ai_job_cancel_response_state(d.pop("state"))

        ai_job_cancel_response = cls(
            job_id=job_id,
            state=state,
        )

        ai_job_cancel_response.additional_properties = d
        return ai_job_cancel_response

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
