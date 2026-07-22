from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_run_body_cache import QueryRunBodyCache, check_query_run_body_cache
from ..models.query_run_body_result_type import QueryRunBodyResultType, check_query_run_body_result_type
from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryRunBody")


@_attrs_define
class QueryRunBody:
    """
    Attributes:
        branch_id (UUID | Unset): Optional model branch to run the query against. Must belong to the same shared model
            as the query. When omitted, the query runs against the shared model. Takes precedence over the legacy
            `?branch_id=` URL query parameter. Example: 550e8400-e29b-41d4-a716-446655440000.
        cache (QueryRunBodyCache | Unset): Cache policy for query execution. Controls whether to use cached results.
            Example: normal.
        environment_connection_id (UUID | Unset): Connection ID of the environment to run the query against, overriding
            the connection environment inherited from the (target) user's session or default. Must be a configured
            environment of the query model's connection that the user can access. Example:
            550e8400-e29b-41d4-a716-446655440000.
        format_results (bool | Unset): Whether to format result values (e.g., apply number formatting). Only valid when
            resultType is specified.
        plan_only (bool | Unset): If true, returns only the query execution plan without running the query. Default:
            False.
        query (Any | Unset): The semantic query definition including fields, filters, sorts, and other query parameters.
        result_type (QueryRunBodyResultType | Unset): Output format for the results. If not specified, returns
            base64-encoded Arrow format.
        user_id (UUID | Unset): Alternate location for the `?userId=` query parameter. Prefer the query parameter — this
            body field exists for backwards compatibility. Supplying both forms results in a 400. Only valid for org-scoped
            API keys; when set, the user's attributes are applied for row-level security and connection-environment
            switching. Example: 550e8400-e29b-41d4-a716-446655440000.
    """

    branch_id: UUID | Unset = UNSET
    cache: QueryRunBodyCache | Unset = UNSET
    environment_connection_id: UUID | Unset = UNSET
    format_results: bool | Unset = UNSET
    plan_only: bool | Unset = False
    query: Any | Unset = UNSET
    result_type: QueryRunBodyResultType | Unset = UNSET
    user_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        cache: str | Unset = UNSET
        if not isinstance(self.cache, Unset):
            cache = self.cache

        environment_connection_id: str | Unset = UNSET
        if not isinstance(self.environment_connection_id, Unset):
            environment_connection_id = str(self.environment_connection_id)

        format_results = self.format_results

        plan_only = self.plan_only

        query = self.query

        result_type: str | Unset = UNSET
        if not isinstance(self.result_type, Unset):
            result_type = self.result_type

        user_id: str | Unset = UNSET
        if not isinstance(self.user_id, Unset):
            user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if cache is not UNSET:
            field_dict["cache"] = cache
        if environment_connection_id is not UNSET:
            field_dict["environmentConnectionId"] = environment_connection_id
        if format_results is not UNSET:
            field_dict["formatResults"] = format_results
        if plan_only is not UNSET:
            field_dict["planOnly"] = plan_only
        if query is not UNSET:
            field_dict["query"] = query
        if result_type is not UNSET:
            field_dict["resultType"] = result_type
        if user_id is not UNSET:
            field_dict["userId"] = user_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        _cache = d.pop("cache", UNSET)
        cache: QueryRunBodyCache | Unset
        if isinstance(_cache, Unset):
            cache = UNSET
        else:
            cache = check_query_run_body_cache(_cache)

        _environment_connection_id = d.pop("environmentConnectionId", UNSET)
        environment_connection_id: UUID | Unset
        if isinstance(_environment_connection_id, Unset):
            environment_connection_id = UNSET
        else:
            environment_connection_id = UUID(_environment_connection_id)

        format_results = d.pop("formatResults", UNSET)

        plan_only = d.pop("planOnly", UNSET)

        query = d.pop("query", UNSET)

        _result_type = d.pop("resultType", UNSET)
        result_type: QueryRunBodyResultType | Unset
        if isinstance(_result_type, Unset):
            result_type = UNSET
        else:
            result_type = check_query_run_body_result_type(_result_type)

        _user_id = d.pop("userId", UNSET)
        user_id: UUID | Unset
        if isinstance(_user_id, Unset):
            user_id = UNSET
        else:
            user_id = UUID(_user_id)

        query_run_body = cls(
            branch_id=branch_id,
            cache=cache,
            environment_connection_id=environment_connection_id,
            format_results=format_results,
            plan_only=plan_only,
            query=query,
            result_type=result_type,
            user_id=user_id,
        )

        query_run_body.additional_properties = d
        return query_run_body

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
