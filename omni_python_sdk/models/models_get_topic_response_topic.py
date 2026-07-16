from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.models_get_topic_response_topic_relationships_item import ModelsGetTopicResponseTopicRelationshipsItem
    from ..models.models_get_topic_response_topic_views_item import ModelsGetTopicResponseTopicViewsItem


T = TypeVar("T", bound="ModelsGetTopicResponseTopic")


@_attrs_define
class ModelsGetTopicResponseTopic:
    """Topic details with relationships and views

    Attributes:
        base_view_name (str): Base view name for the topic
        name (str): Topic name
        relationships (list[ModelsGetTopicResponseTopicRelationshipsItem]): Relationships for the topic
        views (list[ModelsGetTopicResponseTopicViewsItem]): Views available in the topic
        description (str | Unset): Topic description
        group_label (str | Unset): Group label
        hidden (bool | Unset): Whether the topic is hidden
        label (str | Unset): Topic label
    """

    base_view_name: str
    name: str
    relationships: list[ModelsGetTopicResponseTopicRelationshipsItem]
    views: list[ModelsGetTopicResponseTopicViewsItem]
    description: str | Unset = UNSET
    group_label: str | Unset = UNSET
    hidden: bool | Unset = UNSET
    label: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_view_name = self.base_view_name

        name = self.name

        relationships = []
        for relationships_item_data in self.relationships:
            relationships_item = relationships_item_data.to_dict()
            relationships.append(relationships_item)

        views = []
        for views_item_data in self.views:
            views_item = views_item_data.to_dict()
            views.append(views_item)

        description = self.description

        group_label = self.group_label

        hidden = self.hidden

        label = self.label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "base_view_name": base_view_name,
                "name": name,
                "relationships": relationships,
                "views": views,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if group_label is not UNSET:
            field_dict["group_label"] = group_label
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if label is not UNSET:
            field_dict["label"] = label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_get_topic_response_topic_relationships_item import (
            ModelsGetTopicResponseTopicRelationshipsItem,
        )
        from ..models.models_get_topic_response_topic_views_item import ModelsGetTopicResponseTopicViewsItem

        d = dict(src_dict)
        base_view_name = d.pop("base_view_name")

        name = d.pop("name")

        relationships = []
        _relationships = d.pop("relationships")
        for relationships_item_data in _relationships:
            relationships_item = ModelsGetTopicResponseTopicRelationshipsItem.from_dict(relationships_item_data)

            relationships.append(relationships_item)

        views = []
        _views = d.pop("views")
        for views_item_data in _views:
            views_item = ModelsGetTopicResponseTopicViewsItem.from_dict(views_item_data)

            views.append(views_item)

        description = d.pop("description", UNSET)

        group_label = d.pop("group_label", UNSET)

        hidden = d.pop("hidden", UNSET)

        label = d.pop("label", UNSET)

        models_get_topic_response_topic = cls(
            base_view_name=base_view_name,
            name=name,
            relationships=relationships,
            views=views,
            description=description,
            group_label=group_label,
            hidden=hidden,
            label=label,
        )

        models_get_topic_response_topic.additional_properties = d
        return models_get_topic_response_topic

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
