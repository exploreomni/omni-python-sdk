from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dashboards_update_filters_body_controls import DashboardsUpdateFiltersBodyControls
    from ..models.dashboards_update_filters_body_filters import DashboardsUpdateFiltersBodyFilters


T = TypeVar("T", bound="DashboardsUpdateFiltersBody")


@_attrs_define
class DashboardsUpdateFiltersBody:
    """
    Attributes:
        clear_existing_draft (bool | Unset): When true, discards any existing draft before applying updates. Required
            when updating a published document that already has a draft. Default: False.
        controls (DashboardsUpdateFiltersBodyControls | Unset): Partial control updates. Keys are control IDs that must
            exist in the dashboard.
        filter_order (list[str] | Unset): New order for filters. All filter IDs must exist in the dashboard.
        filters (DashboardsUpdateFiltersBodyFilters | Unset): Partial filter updates. Keys are filter IDs that must
            exist in the dashboard.
    """

    clear_existing_draft: bool | Unset = False
    controls: DashboardsUpdateFiltersBodyControls | Unset = UNSET
    filter_order: list[str] | Unset = UNSET
    filters: DashboardsUpdateFiltersBodyFilters | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        clear_existing_draft = self.clear_existing_draft

        controls: dict[str, Any] | Unset = UNSET
        if not isinstance(self.controls, Unset):
            controls = self.controls.to_dict()

        filter_order: list[str] | Unset = UNSET
        if not isinstance(self.filter_order, Unset):
            filter_order = self.filter_order

        filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filters, Unset):
            filters = self.filters.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if clear_existing_draft is not UNSET:
            field_dict["clearExistingDraft"] = clear_existing_draft
        if controls is not UNSET:
            field_dict["controls"] = controls
        if filter_order is not UNSET:
            field_dict["filterOrder"] = filter_order
        if filters is not UNSET:
            field_dict["filters"] = filters

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dashboards_update_filters_body_controls import DashboardsUpdateFiltersBodyControls
        from ..models.dashboards_update_filters_body_filters import DashboardsUpdateFiltersBodyFilters

        d = dict(src_dict)
        clear_existing_draft = d.pop("clearExistingDraft", UNSET)

        _controls = d.pop("controls", UNSET)
        controls: DashboardsUpdateFiltersBodyControls | Unset
        if isinstance(_controls, Unset):
            controls = UNSET
        else:
            controls = DashboardsUpdateFiltersBodyControls.from_dict(_controls)

        filter_order = cast(list[str], d.pop("filterOrder", UNSET))

        _filters = d.pop("filters", UNSET)
        filters: DashboardsUpdateFiltersBodyFilters | Unset
        if isinstance(_filters, Unset):
            filters = UNSET
        else:
            filters = DashboardsUpdateFiltersBodyFilters.from_dict(_filters)

        dashboards_update_filters_body = cls(
            clear_existing_draft=clear_existing_draft,
            controls=controls,
            filter_order=filter_order,
            filters=filters,
        )

        dashboards_update_filters_body.additional_properties = d
        return dashboards_update_filters_body

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
