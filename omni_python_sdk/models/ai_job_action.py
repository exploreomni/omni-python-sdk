from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ai_job_action_query_result import AiJobActionQueryResult


T = TypeVar("T", bound="AiJobAction")


@_attrs_define
class AiJobAction:
    """
    Attributes:
        message (str): The AI's explanation of what it is doing in this step, written in natural language. Example: I'll
            generate a query to find the top 5 products by total revenue..
        timestamp (str): ISO 8601 timestamp when this action occurred. Example: 2025-01-15T10:00:10.000Z.
        type_ (str): The type of action. Common types include "generate_query" (query generation and execution) and
            "summarize" (final answer synthesis). Example: generate_query.
        result (AiJobActionQueryResult | Unset): Query result data. Only present for generate_query action types.
    """

    message: str
    timestamp: str
    type_: str
    result: AiJobActionQueryResult | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        timestamp = self.timestamp

        type_ = self.type_

        result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.result, Unset):
            result = self.result.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
                "timestamp": timestamp,
                "type": type_,
            }
        )
        if result is not UNSET:
            field_dict["result"] = result

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_job_action_query_result import AiJobActionQueryResult

        d = dict(src_dict)
        message = d.pop("message")

        timestamp = d.pop("timestamp")

        type_ = d.pop("type")

        _result = d.pop("result", UNSET)
        result: AiJobActionQueryResult | Unset
        if isinstance(_result, Unset):
            result = UNSET
        else:
            result = AiJobActionQueryResult.from_dict(_result)

        ai_job_action = cls(
            message=message,
            timestamp=timestamp,
            type_=type_,
            result=result,
        )

        ai_job_action.additional_properties = d
        return ai_job_action

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
