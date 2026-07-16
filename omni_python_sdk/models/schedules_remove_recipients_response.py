from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SchedulesRemoveRecipientsResponse")


@_attrs_define
class SchedulesRemoveRecipientsResponse:
    """
    Attributes:
        removed_group_recipients_count (float): Number of user group recipients removed. Example: 1.
        removed_recipients_count (float): Number of individual recipients removed. Example: 2.
        success (bool): Whether the operation was successful. Example: True.
    """

    removed_group_recipients_count: float
    removed_recipients_count: float
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        removed_group_recipients_count = self.removed_group_recipients_count

        removed_recipients_count = self.removed_recipients_count

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "removedGroupRecipientsCount": removed_group_recipients_count,
                "removedRecipientsCount": removed_recipients_count,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        removed_group_recipients_count = d.pop("removedGroupRecipientsCount")

        removed_recipients_count = d.pop("removedRecipientsCount")

        success = d.pop("success")

        schedules_remove_recipients_response = cls(
            removed_group_recipients_count=removed_group_recipients_count,
            removed_recipients_count=removed_recipients_count,
            success=success,
        )

        schedules_remove_recipients_response.additional_properties = d
        return schedules_remove_recipients_response

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
