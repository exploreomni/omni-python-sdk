from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiJobStatusResponseError")


@_attrs_define
class AiJobStatusResponseError:
    """Error details explaining why the job failed. Only present in FAILED state.

    Attributes:
        message (str): Human-readable error message. Example: Column 'revenue' not found in table 'orders'.
        code (str | Unset): Machine-readable error code. Example: QUERY_EXECUTION_ERROR.
        detail (str | Unset): Additional error detail or context. Example: The query timed out after 300 seconds.
    """

    message: str
    code: str | Unset = UNSET
    detail: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        code = self.code

        detail = self.detail

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
            }
        )
        if code is not UNSET:
            field_dict["code"] = code
        if detail is not UNSET:
            field_dict["detail"] = detail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message")

        code = d.pop("code", UNSET)

        detail = d.pop("detail", UNSET)

        ai_job_status_response_error = cls(
            message=message,
            code=code,
            detail=detail,
        )

        ai_job_status_response_error.additional_properties = d
        return ai_job_status_response_error

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
