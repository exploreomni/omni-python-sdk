from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EvalPromptSetsDeleteResponse")


@_attrs_define
class EvalPromptSetsDeleteResponse:
    """
    Attributes:
        cancelled_job_count (int): Number of in-flight agentic jobs associated with this prompt set that were cancelled
            as part of the archive.
        is_archived (bool): Always `true` on success — archives the prompt set.
    """

    cancelled_job_count: int
    is_archived: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cancelled_job_count = self.cancelled_job_count

        is_archived = self.is_archived

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "cancelled_job_count": cancelled_job_count,
                "is_archived": is_archived,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cancelled_job_count = d.pop("cancelled_job_count")

        is_archived = d.pop("is_archived")

        eval_prompt_sets_delete_response = cls(
            cancelled_job_count=cancelled_job_count,
            is_archived=is_archived,
        )

        eval_prompt_sets_delete_response.additional_properties = d
        return eval_prompt_sets_delete_response

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
