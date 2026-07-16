from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiJobSubmitResponse")


@_attrs_define
class AiJobSubmitResponse:
    """
    Attributes:
        conversation_id (UUID): The conversation ID for this job. Pass this as conversationId in subsequent job
            submissions to continue the conversation with additional context. Example: 660e8400-e29b-41d4-a716-446655440001.
        job_id (UUID): The unique identifier for the created job. Use this to poll status via GET
            /api/v1/ai/jobs/{jobId} or retrieve results via GET /api/v1/ai/jobs/{jobId}/result. Example:
            550e8400-e29b-41d4-a716-446655440000.
        omni_chat_url (str): URL to view this conversation in the Omni chat interface. Opens the chat session where the
            job actions and results are visible. Example: https://my-org.omni.co/chat/660e8400-e29b-41d4-a716-446655440001.
    """

    conversation_id: UUID
    job_id: UUID
    omni_chat_url: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        conversation_id = str(self.conversation_id)

        job_id = str(self.job_id)

        omni_chat_url = self.omni_chat_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conversationId": conversation_id,
                "jobId": job_id,
                "omniChatUrl": omni_chat_url,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        conversation_id = UUID(d.pop("conversationId"))

        job_id = UUID(d.pop("jobId"))

        omni_chat_url = d.pop("omniChatUrl")

        ai_job_submit_response = cls(
            conversation_id=conversation_id,
            job_id=job_id,
            omni_chat_url=omni_chat_url,
        )

        ai_job_submit_response.additional_properties = d
        return ai_job_submit_response

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
