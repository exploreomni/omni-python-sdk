from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsUpdateTopicBody")


@_attrs_define
class ModelsUpdateTopicBody:
    """
    Attributes:
        description (str | Unset): Topic description
        group_label (str | Unset): Group label for the topic
        hidden (bool | Unset): Whether the topic is hidden
        label (str | Unset): Topic label
        new_topic_name (str | Unset): New topic name (for rename)
    """

    description: str | Unset = UNSET
    group_label: str | Unset = UNSET
    hidden: bool | Unset = UNSET
    label: str | Unset = UNSET
    new_topic_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        group_label = self.group_label

        hidden = self.hidden

        label = self.label

        new_topic_name = self.new_topic_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if group_label is not UNSET:
            field_dict["groupLabel"] = group_label
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if label is not UNSET:
            field_dict["label"] = label
        if new_topic_name is not UNSET:
            field_dict["newTopicName"] = new_topic_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        description = d.pop("description", UNSET)

        group_label = d.pop("groupLabel", UNSET)

        hidden = d.pop("hidden", UNSET)

        label = d.pop("label", UNSET)

        new_topic_name = d.pop("newTopicName", UNSET)

        models_update_topic_body = cls(
            description=description,
            group_label=group_label,
            hidden=hidden,
            label=label,
            new_topic_name=new_topic_name,
        )

        models_update_topic_body.additional_properties = d
        return models_update_topic_body

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
