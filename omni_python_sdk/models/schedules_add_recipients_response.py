from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SchedulesAddRecipientsResponse")


@_attrs_define
class SchedulesAddRecipientsResponse:
    """
    Attributes:
        added_group_recipients_count (float): Number of user group recipients added. Example: 1.
        added_recipients_count (float): Number of individual recipients added. Example: 2.
        success (bool): Whether the operation was successful. Example: True.
    """

    added_group_recipients_count: float
    added_recipients_count: float
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        added_group_recipients_count = self.added_group_recipients_count

        added_recipients_count = self.added_recipients_count

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "addedGroupRecipientsCount": added_group_recipients_count,
                "addedRecipientsCount": added_recipients_count,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        added_group_recipients_count = d.pop("addedGroupRecipientsCount")

        added_recipients_count = d.pop("addedRecipientsCount")

        success = d.pop("success")

        schedules_add_recipients_response = cls(
            added_group_recipients_count=added_group_recipients_count,
            added_recipients_count=added_recipients_count,
            success=success,
        )

        schedules_add_recipients_response.additional_properties = d
        return schedules_add_recipients_response

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
