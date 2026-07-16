from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="RoutineLastRunType0")


@_attrs_define
class RoutineLastRunType0:
    """Most recent completed run, or null if the routine has never completed a run.

    Attributes:
        completed_at (None | str): ISO 8601 timestamp the last completed run finished.
        label (str): Customer-visible status of the last completed run. Example: Delivered.
        state (str): Machine-readable status of the last completed run. Example: COMPLETE.
    """

    completed_at: None | str
    label: str
    state: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        completed_at: None | str
        completed_at = self.completed_at

        label = self.label

        state = self.state

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "completedAt": completed_at,
                "label": label,
                "state": state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_completed_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        completed_at = _parse_completed_at(d.pop("completedAt"))

        label = d.pop("label")

        state = d.pop("state")

        routine_last_run_type_0 = cls(
            completed_at=completed_at,
            label=label,
            state=state,
        )

        routine_last_run_type_0.additional_properties = d
        return routine_last_run_type_0

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
