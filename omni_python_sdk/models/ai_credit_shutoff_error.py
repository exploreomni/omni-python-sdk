from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_credit_shutoff_error_code import AiCreditShutoffErrorCode, check_ai_credit_shutoff_error_code

T = TypeVar("T", bound="AiCreditShutoffError")


@_attrs_define
class AiCreditShutoffError:
    """
    Attributes:
        code (AiCreditShutoffErrorCode): Stable reason code identifying an AI-credit shutoff. Example: shutoff.
        detail (str): Human-readable error message describing what went wrong. Example: The AI credit limit has been
            reached. Contact your administrator for assistance..
        status (int): HTTP status code of the error. Example: 402.
    """

    code: AiCreditShutoffErrorCode
    detail: str
    status: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code: str = self.code

        detail = self.detail

        status = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "detail": detail,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = check_ai_credit_shutoff_error_code(d.pop("code"))

        detail = d.pop("detail")

        status = d.pop("status")

        ai_credit_shutoff_error = cls(
            code=code,
            detail=detail,
            status=status,
        )

        ai_credit_shutoff_error.additional_properties = d
        return ai_credit_shutoff_error

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
