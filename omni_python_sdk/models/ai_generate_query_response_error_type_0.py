from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiGenerateQueryResponseErrorType0")


@_attrs_define
class AiGenerateQueryResponseErrorType0:
    """Error details if query generation failed. Null on success.

    Attributes:
        detail (str): Detailed error message explaining why query generation failed. Example: The AI was unable to
            generate a query for this prompt. Try rephrasing your question to be more specific about the data you want to
            retrieve..
        message (str): Short error summary. Example: No query generated.
    """

    detail: str
    message: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        detail = self.detail

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "detail": detail,
                "message": message,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        detail = d.pop("detail")

        message = d.pop("message")

        ai_generate_query_response_error_type_0 = cls(
            detail=detail,
            message=message,
        )

        ai_generate_query_response_error_type_0.additional_properties = d
        return ai_generate_query_response_error_type_0

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
