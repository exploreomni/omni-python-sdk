from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SchedulesTransferOwnershipBody")


@_attrs_define
class SchedulesTransferOwnershipBody:
    """
    Attributes:
        user_id (UUID): The UUID of the user to transfer schedule ownership to. Use the List users endpoint to retrieve
            user IDs. The new owner must be a member of the same organization, not be the current owner, and have permission
            to view the dashboard associated with the schedule. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
    """

    user_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "userId": user_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        user_id = UUID(d.pop("userId"))

        schedules_transfer_ownership_body = cls(
            user_id=user_id,
        )

        schedules_transfer_ownership_body.additional_properties = d
        return schedules_transfer_ownership_body

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
