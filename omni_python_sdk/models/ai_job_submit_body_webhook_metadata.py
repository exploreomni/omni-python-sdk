from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiJobSubmitBodyWebhookMetadata")


@_attrs_define
class AiJobSubmitBodyWebhookMetadata:
    """Arbitrary metadata object that will be included unchanged in webhook payloads. Use this to correlate webhook
    notifications with your own system (e.g., tracking IDs, channel references).

        Example:
            {'externalId': 'task-123', 'slackChannel': 'C0123456789'}

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        ai_job_submit_body_webhook_metadata = cls()

        ai_job_submit_body_webhook_metadata.additional_properties = d
        return ai_job_submit_body_webhook_metadata

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
