from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SchedulesCreateSchedulesCreateResponse")


@_attrs_define
class SchedulesCreateSchedulesCreateResponse:
    """Create schedule response

    Attributes:
        message (str): Success message Example: Successfully created schedule.
        deliverer_role_arn (str | Unset): The ARN of the Omni deliverer role. Use this as the Principal in your IAM role
            trust policy. Only returned for S3 destinations. Example:
            arn:aws:iam::529831494235:role/OmniSchedulerDelivererRole.
        external_id (UUID | Unset): The organization ID used as the external ID for confused deputy prevention. Add this
            to your IAM role trust policy as the sts:ExternalId condition. Static across all S3 destinations for your
            organization. Only returned for S3 destinations. Example: 550e8400-e29b-41d4-a716-446655440000.
        id (UUID | Unset): Created schedule ID (only when testNow is false) Example:
            123e4567-e89b-12d3-a456-426614174000.
    """

    message: str
    deliverer_role_arn: str | Unset = UNSET
    external_id: UUID | Unset = UNSET
    id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        deliverer_role_arn = self.deliverer_role_arn

        external_id: str | Unset = UNSET
        if not isinstance(self.external_id, Unset):
            external_id = str(self.external_id)

        id: str | Unset = UNSET
        if not isinstance(self.id, Unset):
            id = str(self.id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
            }
        )
        if deliverer_role_arn is not UNSET:
            field_dict["delivererRoleArn"] = deliverer_role_arn
        if external_id is not UNSET:
            field_dict["externalId"] = external_id
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message")

        deliverer_role_arn = d.pop("delivererRoleArn", UNSET)

        _external_id = d.pop("externalId", UNSET)
        external_id: UUID | Unset
        if isinstance(_external_id, Unset):
            external_id = UNSET
        else:
            external_id = UUID(_external_id)

        _id = d.pop("id", UNSET)
        id: UUID | Unset
        if isinstance(_id, Unset):
            id = UNSET
        else:
            id = UUID(_id)

        schedules_create_schedules_create_response = cls(
            message=message,
            deliverer_role_arn=deliverer_role_arn,
            external_id=external_id,
            id=id,
        )

        schedules_create_schedules_create_response.additional_properties = d
        return schedules_create_schedules_create_response

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
