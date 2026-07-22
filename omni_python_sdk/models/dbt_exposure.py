from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.dbt_exposure_type import DbtExposureType, check_dbt_exposure_type
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dbt_exposure_owner import DbtExposureOwner


T = TypeVar("T", bound="DbtExposure")


@_attrs_define
class DbtExposure:
    """The dbt exposure for this dashboard.

    Attributes:
        depends_on (list[str]): List of dbt model references (e.g. ref('model_name')) Example: ["ref('orders')",
            "ref('customers')"].
        name (str): Sanitized exposure name. May contain duplicates across exposures; use deduplication_name for a
            guaranteed-unique alternative. Example: my_dashboard.
        owner (DbtExposureOwner):
        type_ (DbtExposureType): Type of the exposure Example: dashboard.
        label (str | Unset): Original dashboard name
        url (str | Unset): URL of the dashboard
    """

    depends_on: list[str]
    name: str
    owner: DbtExposureOwner
    type_: DbtExposureType
    label: str | Unset = UNSET
    url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        depends_on = self.depends_on

        name = self.name

        owner = self.owner.to_dict()

        type_: str = self.type_

        label = self.label

        url = self.url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "depends_on": depends_on,
                "name": name,
                "owner": owner,
                "type": type_,
            }
        )
        if label is not UNSET:
            field_dict["label"] = label
        if url is not UNSET:
            field_dict["url"] = url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dbt_exposure_owner import DbtExposureOwner

        d = dict(src_dict)
        depends_on = cast(list[str], d.pop("depends_on"))

        name = d.pop("name")

        owner = DbtExposureOwner.from_dict(d.pop("owner"))

        type_ = check_dbt_exposure_type(d.pop("type"))

        label = d.pop("label", UNSET)

        url = d.pop("url", UNSET)

        dbt_exposure = cls(
            depends_on=depends_on,
            name=name,
            owner=owner,
            type_=type_,
            label=label,
            url=url,
        )

        dbt_exposure.additional_properties = d
        return dbt_exposure

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
