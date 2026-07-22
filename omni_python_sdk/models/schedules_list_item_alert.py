from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SchedulesListItemAlert")


@_attrs_define
class SchedulesListItemAlert:
    """Alert configuration (only present for alert-type schedules)

    Attributes:
        condition_query_name (None | str): Name of the query used for alert condition
        condition_type (str): Type of alert condition: RESULTS_CHANGED, RESULTS_PRESENT, RESULTS_MISSING
    """

    condition_query_name: None | str
    condition_type: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        condition_query_name: None | str
        condition_query_name = self.condition_query_name

        condition_type = self.condition_type

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conditionQueryName": condition_query_name,
                "conditionType": condition_type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_condition_query_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        condition_query_name = _parse_condition_query_name(d.pop("conditionQueryName"))

        condition_type = d.pop("conditionType")

        schedules_list_item_alert = cls(
            condition_query_name=condition_query_name,
            condition_type=condition_type,
        )

        schedules_list_item_alert.additional_properties = d
        return schedules_list_item_alert

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
