from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.models_get_view_response_views_item_fields_item import ModelsGetViewResponseViewsItemFieldsItem


T = TypeVar("T", bound="ModelsGetViewResponseViewsItem")


@_attrs_define
class ModelsGetViewResponseViewsItem:
    """
    Attributes:
        fields (list[ModelsGetViewResponseViewsItemFieldsItem]): Fields in the view
        name (str): View name
        description (str | Unset): View description
        hidden (bool | Unset): Whether the view is hidden
        label (str | Unset): View label
    """

    fields: list[ModelsGetViewResponseViewsItemFieldsItem]
    name: str
    description: str | Unset = UNSET
    hidden: bool | Unset = UNSET
    label: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields = []
        for fields_item_data in self.fields:
            fields_item = fields_item_data.to_dict()
            fields.append(fields_item)

        name = self.name

        description = self.description

        hidden = self.hidden

        label = self.label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "fields": fields,
                "name": name,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if label is not UNSET:
            field_dict["label"] = label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_get_view_response_views_item_fields_item import ModelsGetViewResponseViewsItemFieldsItem

        d = dict(src_dict)
        fields = []
        _fields = d.pop("fields")
        for fields_item_data in _fields:
            fields_item = ModelsGetViewResponseViewsItemFieldsItem.from_dict(fields_item_data)

            fields.append(fields_item)

        name = d.pop("name")

        description = d.pop("description", UNSET)

        hidden = d.pop("hidden", UNSET)

        label = d.pop("label", UNSET)

        models_get_view_response_views_item = cls(
            fields=fields,
            name=name,
            description=description,
            hidden=hidden,
            label=label,
        )

        models_get_view_response_views_item.additional_properties = d
        return models_get_view_response_views_item

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
