from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsUpdateViewBody")


@_attrs_define
class ModelsUpdateViewBody:
    """
    Attributes:
        ai_context (str | Unset): AI context for the view
        description (str | Unset): View description
        format_ (str | Unset): View format
        hidden (bool | Unset): Whether the view is hidden
        label (str | Unset): View label
        tags (list[str] | Unset): Tags for the view
    """

    ai_context: str | Unset = UNSET
    description: str | Unset = UNSET
    format_: str | Unset = UNSET
    hidden: bool | Unset = UNSET
    label: str | Unset = UNSET
    tags: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ai_context = self.ai_context

        description = self.description

        format_ = self.format_

        hidden = self.hidden

        label = self.label

        tags: list[str] | Unset = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if ai_context is not UNSET:
            field_dict["aiContext"] = ai_context
        if description is not UNSET:
            field_dict["description"] = description
        if format_ is not UNSET:
            field_dict["format"] = format_
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if label is not UNSET:
            field_dict["label"] = label
        if tags is not UNSET:
            field_dict["tags"] = tags

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ai_context = d.pop("aiContext", UNSET)

        description = d.pop("description", UNSET)

        format_ = d.pop("format", UNSET)

        hidden = d.pop("hidden", UNSET)

        label = d.pop("label", UNSET)

        tags = cast(list[str], d.pop("tags", UNSET))

        models_update_view_body = cls(
            ai_context=ai_context,
            description=description,
            format_=format_,
            hidden=hidden,
            label=label,
            tags=tags,
        )

        models_update_view_body.additional_properties = d
        return models_update_view_body

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
