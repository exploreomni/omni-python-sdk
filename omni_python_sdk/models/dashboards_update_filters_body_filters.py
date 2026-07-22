from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.dashboards_update_filters_body_filters_additional_property import (
        DashboardsUpdateFiltersBodyFiltersAdditionalProperty,
    )


T = TypeVar("T", bound="DashboardsUpdateFiltersBodyFilters")


@_attrs_define
class DashboardsUpdateFiltersBodyFilters:
    """Partial filter updates. Keys are filter IDs that must exist in the dashboard."""

    additional_properties: dict[str, DashboardsUpdateFiltersBodyFiltersAdditionalProperty] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dashboards_update_filters_body_filters_additional_property import (
            DashboardsUpdateFiltersBodyFiltersAdditionalProperty,
        )

        d = dict(src_dict)
        dashboards_update_filters_body_filters = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = DashboardsUpdateFiltersBodyFiltersAdditionalProperty.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        dashboards_update_filters_body_filters.additional_properties = additional_properties
        return dashboards_update_filters_body_filters

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> DashboardsUpdateFiltersBodyFiltersAdditionalProperty:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: DashboardsUpdateFiltersBodyFiltersAdditionalProperty) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
