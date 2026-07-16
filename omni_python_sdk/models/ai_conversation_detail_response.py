from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.ai_conversation_message import AiConversationMessage


T = TypeVar("T", bound="AiConversationDetailResponse")


@_attrs_define
class AiConversationDetailResponse:
    """
    Attributes:
        created_at (datetime.datetime):
        id (UUID):
        messages (list[AiConversationMessage]): Messages in chronological order. Alternating user / assistant turns.
        name (None | str):
        updated_at (datetime.datetime):
    """

    created_at: datetime.datetime
    id: UUID
    messages: list[AiConversationMessage]
    name: None | str
    updated_at: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at = self.created_at.isoformat()

        id = str(self.id)

        messages = []
        for messages_item_data in self.messages:
            messages_item = messages_item_data.to_dict()
            messages.append(messages_item)

        name: None | str
        name = self.name

        updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "createdAt": created_at,
                "id": id,
                "messages": messages,
                "name": name,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_conversation_message import AiConversationMessage

        d = dict(src_dict)
        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        id = UUID(d.pop("id"))

        messages = []
        _messages = d.pop("messages")
        for messages_item_data in _messages:
            messages_item = AiConversationMessage.from_dict(messages_item_data)

            messages.append(messages_item)

        def _parse_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        name = _parse_name(d.pop("name"))

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        ai_conversation_detail_response = cls(
            created_at=created_at,
            id=id,
            messages=messages,
            name=name,
            updated_at=updated_at,
        )

        ai_conversation_detail_response.additional_properties = d
        return ai_conversation_detail_response

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
