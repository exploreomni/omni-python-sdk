from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.models_get_view_response_views_item import ModelsGetViewResponseViewsItem


T = TypeVar("T", bound="ModelsGetViewResponse")


@_attrs_define
class ModelsGetViewResponse:
    """
    Attributes:
        success (bool): Whether the operation succeeded
        views (list[ModelsGetViewResponseViewsItem]): List of views
    """

    success: bool
    views: list[ModelsGetViewResponseViewsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        success = self.success

        views = []
        for views_item_data in self.views:
            views_item = views_item_data.to_dict()
            views.append(views_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "success": success,
                "views": views,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_get_view_response_views_item import ModelsGetViewResponseViewsItem

        d = dict(src_dict)
        success = d.pop("success")

        views = []
        _views = d.pop("views")
        for views_item_data in _views:
            views_item = ModelsGetViewResponseViewsItem.from_dict(views_item_data)

            views.append(views_item)

        models_get_view_response = cls(
            success=success,
            views=views,
        )

        models_get_view_response.additional_properties = d
        return models_get_view_response

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
