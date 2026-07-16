from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiConversation")


@_attrs_define
class AiConversation:
    """
    Attributes:
        created_at (datetime.datetime): When the conversation was started. Example: 2025-01-15T10:00:00.000Z.
        id (UUID): Conversation ID. Pass as conversationId on subsequent /api/v1/ai/jobs submissions to continue this
            conversation. Example: 660e8400-e29b-41d4-a716-446655440001.
        last_prompt (None | str): The most recent user prompt in this conversation, useful for displaying a one-line
            summary in a list. Example: What were our top products last week?.
        name (None | str): Conversation title. Set by the AI after the first turn; null on brand-new sessions. Example:
            Top products last week.
        updated_at (datetime.datetime): When the conversation was last touched (most recent prompt or AI activity).
            Example: 2025-01-15T10:01:30.000Z.
    """

    created_at: datetime.datetime
    id: UUID
    last_prompt: None | str
    name: None | str
    updated_at: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at = self.created_at.isoformat()

        id = str(self.id)

        last_prompt: None | str
        last_prompt = self.last_prompt

        name: None | str
        name = self.name

        updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "createdAt": created_at,
                "id": id,
                "lastPrompt": last_prompt,
                "name": name,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        id = UUID(d.pop("id"))

        def _parse_last_prompt(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        last_prompt = _parse_last_prompt(d.pop("lastPrompt"))

        def _parse_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        name = _parse_name(d.pop("name"))

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        ai_conversation = cls(
            created_at=created_at,
            id=id,
            last_prompt=last_prompt,
            name=name,
            updated_at=updated_at,
        )

        ai_conversation.additional_properties = d
        return ai_conversation

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
