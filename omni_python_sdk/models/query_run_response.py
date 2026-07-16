from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryRunResponse")


@_attrs_define
class QueryRunResponse:
    """
    Attributes:
        completed_queries (list[Any] | Unset): Queries that completed synchronously with their results.
        job_ids (list[str] | Unset): Job IDs for queries running asynchronously. Use /api/v1/query/wait to poll for
            results. Example: ['job_abc123', 'job_def456'].
        plan (Any | Unset): Query execution plan (only present if planOnly is true).
    """

    completed_queries: list[Any] | Unset = UNSET
    job_ids: list[str] | Unset = UNSET
    plan: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        completed_queries: list[Any] | Unset = UNSET
        if not isinstance(self.completed_queries, Unset):
            completed_queries = self.completed_queries

        job_ids: list[str] | Unset = UNSET
        if not isinstance(self.job_ids, Unset):
            job_ids = self.job_ids

        plan = self.plan

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if completed_queries is not UNSET:
            field_dict["completedQueries"] = completed_queries
        if job_ids is not UNSET:
            field_dict["jobIds"] = job_ids
        if plan is not UNSET:
            field_dict["plan"] = plan

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        completed_queries = cast(list[Any], d.pop("completedQueries", UNSET))

        job_ids = cast(list[str], d.pop("jobIds", UNSET))

        plan = d.pop("plan", UNSET)

        query_run_response = cls(
            completed_queries=completed_queries,
            job_ids=job_ids,
            plan=plan,
        )

        query_run_response.additional_properties = d
        return query_run_response

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
