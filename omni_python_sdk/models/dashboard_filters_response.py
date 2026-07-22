from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DashboardFiltersResponse")


@_attrs_define
class DashboardFiltersResponse:
    """
    Attributes:
        filter_order (list[str]): Ordered list of filter IDs defining display order Example: ['filter_abc123',
            'filter_def456'].
        identifier (str): Dashboard identifier Example: 12db1a0a.
        controls (Any | Unset): Control configuration object. Keys are control IDs, values contain controlType,
            filterId, label, etc.
        filters (Any | Unset): Filter configuration object. Keys are filter IDs, values contain fieldName, viewName,
            kind, defaultValue, etc.
    """

    filter_order: list[str]
    identifier: str
    controls: Any | Unset = UNSET
    filters: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        filter_order = self.filter_order

        identifier = self.identifier

        controls = self.controls

        filters = self.filters

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "filterOrder": filter_order,
                "identifier": identifier,
            }
        )
        if controls is not UNSET:
            field_dict["controls"] = controls
        if filters is not UNSET:
            field_dict["filters"] = filters

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        filter_order = cast(list[str], d.pop("filterOrder"))

        identifier = d.pop("identifier")

        controls = d.pop("controls", UNSET)

        filters = d.pop("filters", UNSET)

        dashboard_filters_response = cls(
            filter_order=filter_order,
            identifier=identifier,
            controls=controls,
            filters=filters,
        )

        dashboard_filters_response.additional_properties = d
        return dashboard_filters_response

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
