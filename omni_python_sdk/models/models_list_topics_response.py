from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.models_list_topics_response_topics_item import ModelsListTopicsResponseTopicsItem


T = TypeVar("T", bound="ModelsListTopicsResponse")


@_attrs_define
class ModelsListTopicsResponse:
    """
    Attributes:
        success (bool): Whether the operation succeeded
        topics (list[ModelsListTopicsResponseTopicsItem]): List of topics
    """

    success: bool
    topics: list[ModelsListTopicsResponseTopicsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        success = self.success

        topics = []
        for topics_item_data in self.topics:
            topics_item = topics_item_data.to_dict()
            topics.append(topics_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "success": success,
                "topics": topics,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_list_topics_response_topics_item import ModelsListTopicsResponseTopicsItem

        d = dict(src_dict)
        success = d.pop("success")

        topics = []
        _topics = d.pop("topics")
        for topics_item_data in _topics:
            topics_item = ModelsListTopicsResponseTopicsItem.from_dict(topics_item_data)

            topics.append(topics_item)

        models_list_topics_response = cls(
            success=success,
            topics=topics,
        )

        models_list_topics_response.additional_properties = d
        return models_list_topics_response

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
