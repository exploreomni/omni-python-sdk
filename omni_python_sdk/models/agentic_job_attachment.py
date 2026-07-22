from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AgenticJobAttachment")


@_attrs_define
class AgenticJobAttachment:
    """
    Attributes:
        data (str): Base64-encoded file content. Example: iVBORw0KGgoAAAANSUhEUgAA....
        mime_type (str): MIME type of the attachment. Must be an image type (e.g. image/png, image/jpeg) or
            application/pdf. Example: image/png.
        name (str | Unset): Optional filename, used for display/logging only. Example: legacy-dashboard-screenshot.png.
    """

    data: str
    mime_type: str
    name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = self.data

        mime_type = self.mime_type

        name = self.name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "data": data,
                "mimeType": mime_type,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        data = d.pop("data")

        mime_type = d.pop("mimeType")

        name = d.pop("name", UNSET)

        agentic_job_attachment = cls(
            data=data,
            mime_type=mime_type,
            name=name,
        )

        agentic_job_attachment.additional_properties = d
        return agentic_job_attachment

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
