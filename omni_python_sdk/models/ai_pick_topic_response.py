from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiPickTopicResponse")


@_attrs_define
class AiPickTopicResponse:
    """
    Attributes:
        topic_id (str): The name of the topic that best matches the prompt. Use this as the topicName parameter when
            calling generate-query or submitting an AI job. Example: order_items.
    """

    topic_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        topic_id = self.topic_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "topicId": topic_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        topic_id = d.pop("topicId")

        ai_pick_topic_response = cls(
            topic_id=topic_id,
        )

        ai_pick_topic_response.additional_properties = d
        return ai_pick_topic_response

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
