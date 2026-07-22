from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_conversation_message_role import AiConversationMessageRole, check_ai_conversation_message_role

T = TypeVar("T", bound="AiConversationMessage")


@_attrs_define
class AiConversationMessage:
    """
    Attributes:
        created_at (datetime.datetime): When this turn was recorded. Example: 2025-01-15T10:00:00.000Z.
        job_id (None | UUID): The agentic job that produced this assistant turn. Only set for assistant messages —
            clients use it to fetch the rendered chart via GET /api/v1/ai/jobs/{jobId}/vis. Null when the turn predates jobs
            or when we could not associate one. Example: 550e8400-e29b-41d4-a716-446655440000.
        omni_chat_url (None | str): Deep link to the assistant turn in the Omni chat UI. Null for user turns, and for
            assistant turns produced outside the Agentic API (where no AgenticJob row exists). Example: https://my-
            org.omni.co/chat/660e8400-e29b-41d4-a716-446655440001.
        role (AiConversationMessageRole): Speaker — `user` for prompts the user submitted, `assistant` for Blobby's
            responses. Example: user.
        text (str): Markdown content of the message. For assistant turns this is the same string returned by
            /api/v1/ai/jobs/{jobId}/result#message. Example: What were our top products last week?.
    """

    created_at: datetime.datetime
    job_id: None | UUID
    omni_chat_url: None | str
    role: AiConversationMessageRole
    text: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at = self.created_at.isoformat()

        job_id: None | str
        if isinstance(self.job_id, UUID):
            job_id = str(self.job_id)
        else:
            job_id = self.job_id

        omni_chat_url: None | str
        omni_chat_url = self.omni_chat_url

        role: str = self.role

        text = self.text

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "createdAt": created_at,
                "jobId": job_id,
                "omniChatUrl": omni_chat_url,
                "role": role,
                "text": text,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        def _parse_job_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                job_id_type_0 = UUID(data)

                return job_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        job_id = _parse_job_id(d.pop("jobId"))

        def _parse_omni_chat_url(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        omni_chat_url = _parse_omni_chat_url(d.pop("omniChatUrl"))

        role = check_ai_conversation_message_role(d.pop("role"))

        text = d.pop("text")

        ai_conversation_message = cls(
            created_at=created_at,
            job_id=job_id,
            omni_chat_url=omni_chat_url,
            role=role,
            text=text,
        )

        ai_conversation_message.additional_properties = d
        return ai_conversation_message

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
