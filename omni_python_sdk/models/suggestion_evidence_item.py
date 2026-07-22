from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.suggestion_evidence_item_type import SuggestionEvidenceItemType, check_suggestion_evidence_item_type

T = TypeVar("T", bound="SuggestionEvidenceItem")


@_attrs_define
class SuggestionEvidenceItem:
    """
    Attributes:
        captured_at (str): ISO 8601 timestamp of when the evidence was captured.
        chat_ai_session_id (UUID): Chat session that motivated the suggestion.
        type_ (SuggestionEvidenceItemType):
    """

    captured_at: str
    chat_ai_session_id: UUID
    type_: SuggestionEvidenceItemType
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        captured_at = self.captured_at

        chat_ai_session_id = str(self.chat_ai_session_id)

        type_: str = self.type_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "capturedAt": captured_at,
                "chatAiSessionId": chat_ai_session_id,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        captured_at = d.pop("capturedAt")

        chat_ai_session_id = UUID(d.pop("chatAiSessionId"))

        type_ = check_suggestion_evidence_item_type(d.pop("type"))

        suggestion_evidence_item = cls(
            captured_at=captured_at,
            chat_ai_session_id=chat_ai_session_id,
            type_=type_,
        )

        suggestion_evidence_item.additional_properties = d
        return suggestion_evidence_item

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
