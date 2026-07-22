from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.dbt_exposure import DbtExposure


T = TypeVar("T", bound="DbtExposureWithMeta")


@_attrs_define
class DbtExposureWithMeta:
    """
    Attributes:
        dashboard_identifier (str): Identifier of the dashboard that generated this exposure
        deduplication_name (str): A unique name for this exposure. Use this instead of exposure.name to avoid duplicate
            names, or use it as a fallback when exposure.name collides with another exposure.
        exposure (DbtExposure): The dbt exposure for this dashboard.
    """

    dashboard_identifier: str
    deduplication_name: str
    exposure: DbtExposure
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dashboard_identifier = self.dashboard_identifier

        deduplication_name = self.deduplication_name

        exposure = self.exposure.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "dashboard_identifier": dashboard_identifier,
                "deduplication_name": deduplication_name,
                "exposure": exposure,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dbt_exposure import DbtExposure

        d = dict(src_dict)
        dashboard_identifier = d.pop("dashboard_identifier")

        deduplication_name = d.pop("deduplication_name")

        exposure = DbtExposure.from_dict(d.pop("exposure"))

        dbt_exposure_with_meta = cls(
            dashboard_identifier=dashboard_identifier,
            deduplication_name=deduplication_name,
            exposure=exposure,
        )

        dbt_exposure_with_meta.additional_properties = d
        return dbt_exposure_with_meta

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
