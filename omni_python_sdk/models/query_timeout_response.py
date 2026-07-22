from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryTimeoutResponse")


@_attrs_define
class QueryTimeoutResponse:
    """
    Attributes:
        detail (str): Error message indicating the query timed out. Example: Query timed out.
        timed_out (bool): Always true for timeout responses. Example: True.
        remaining_job_ids (list[str] | Unset): Job IDs for queries that have not yet completed. Use /api/v1/query/wait
            to poll for results.
    """

    detail: str
    timed_out: bool
    remaining_job_ids: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        detail = self.detail

        timed_out = self.timed_out

        remaining_job_ids: list[str] | Unset = UNSET
        if not isinstance(self.remaining_job_ids, Unset):
            remaining_job_ids = self.remaining_job_ids

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "detail": detail,
                "timed_out": timed_out,
            }
        )
        if remaining_job_ids is not UNSET:
            field_dict["remaining_job_ids"] = remaining_job_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        detail = d.pop("detail")

        timed_out = d.pop("timed_out")

        remaining_job_ids = cast(list[str], d.pop("remaining_job_ids", UNSET))

        query_timeout_response = cls(
            detail=detail,
            timed_out=timed_out,
            remaining_job_ids=remaining_job_ids,
        )

        query_timeout_response.additional_properties = d
        return query_timeout_response

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
