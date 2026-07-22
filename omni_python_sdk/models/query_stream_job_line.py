from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_stream_job_line_column_name_mapping import QueryStreamJobLineColumnNameMapping
    from ..models.query_stream_job_line_stream_stats import QueryStreamJobLineStreamStats
    from ..models.query_stream_job_line_used_keys import QueryStreamJobLineUsedKeys


T = TypeVar("T", bound="QueryStreamJobLine")


@_attrs_define
class QueryStreamJobLine:
    """Per-job line emitted as each job reaches a terminal state. A completed job carries the result set as base64-encoded
    Arrow IPC in `result`; a failed job carries `error_type` and `error_message` instead.

        Attributes:
            job_id (str): ID of the query job this line reports on.
            status (str): Job status. Known values include COMPLETE, ERROR, FAILED, and MISSING; new values may be added
                over time. Example: COMPLETE.
            cache_metadata (Any | Unset): Cache metadata for the result (row count, byte size, freshness timestamps, requery
                plan key).
            client_result_id (str | Unset): Client-supplied result ID echoed back for correlating jobs to queries.
            column_name_mapping (QueryStreamJobLineColumnNameMapping | Unset):
            error (Any | Unset): Structured error details, e.g. an OAuth re-authentication requirement.
            error_message (str | Unset): Human-readable error message. Present on failed jobs. Example: No such view
                "order_items".
            error_type (str | Unset): Machine-readable error category (e.g. PLAN, SQL). Present on failed jobs. Example:
                PLAN.
            kill_reason (str | Unset): Why the job was killed, when it was cancelled.
            query (Any | Unset): The query that was executed.
            requery_fallback_sql (str | Unset):
            requery_sql (str | Unset): SQL to re-query the cached result set, when the result supports requery.
            requery_table_name (str | Unset):
            result (str | Unset): Result rows as a base64-encoded Arrow IPC stream. Present on completed jobs. Decode with
                any Arrow IPC reader and use summary.fields to interpret the columns.
            stream_stats (QueryStreamJobLineStreamStats | Unset): Server-side streaming latency stats, in milliseconds.
            summary (Any | Unset): Execution summary. summary.fields maps field names to their metadata and is needed to
                interpret the decoded Arrow table; also carries the generated SQL and cache type.
            used_keys (QueryStreamJobLineUsedKeys | Unset):
    """

    job_id: str
    status: str
    cache_metadata: Any | Unset = UNSET
    client_result_id: str | Unset = UNSET
    column_name_mapping: QueryStreamJobLineColumnNameMapping | Unset = UNSET
    error: Any | Unset = UNSET
    error_message: str | Unset = UNSET
    error_type: str | Unset = UNSET
    kill_reason: str | Unset = UNSET
    query: Any | Unset = UNSET
    requery_fallback_sql: str | Unset = UNSET
    requery_sql: str | Unset = UNSET
    requery_table_name: str | Unset = UNSET
    result: str | Unset = UNSET
    stream_stats: QueryStreamJobLineStreamStats | Unset = UNSET
    summary: Any | Unset = UNSET
    used_keys: QueryStreamJobLineUsedKeys | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        status = self.status

        cache_metadata = self.cache_metadata

        client_result_id = self.client_result_id

        column_name_mapping: dict[str, Any] | Unset = UNSET
        if not isinstance(self.column_name_mapping, Unset):
            column_name_mapping = self.column_name_mapping.to_dict()

        error = self.error

        error_message = self.error_message

        error_type = self.error_type

        kill_reason = self.kill_reason

        query = self.query

        requery_fallback_sql = self.requery_fallback_sql

        requery_sql = self.requery_sql

        requery_table_name = self.requery_table_name

        result = self.result

        stream_stats: dict[str, Any] | Unset = UNSET
        if not isinstance(self.stream_stats, Unset):
            stream_stats = self.stream_stats.to_dict()

        summary = self.summary

        used_keys: dict[str, Any] | Unset = UNSET
        if not isinstance(self.used_keys, Unset):
            used_keys = self.used_keys.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_id": job_id,
                "status": status,
            }
        )
        if cache_metadata is not UNSET:
            field_dict["cache_metadata"] = cache_metadata
        if client_result_id is not UNSET:
            field_dict["client_result_id"] = client_result_id
        if column_name_mapping is not UNSET:
            field_dict["column_name_mapping"] = column_name_mapping
        if error is not UNSET:
            field_dict["error"] = error
        if error_message is not UNSET:
            field_dict["error_message"] = error_message
        if error_type is not UNSET:
            field_dict["error_type"] = error_type
        if kill_reason is not UNSET:
            field_dict["kill_reason"] = kill_reason
        if query is not UNSET:
            field_dict["query"] = query
        if requery_fallback_sql is not UNSET:
            field_dict["requery_fallback_sql"] = requery_fallback_sql
        if requery_sql is not UNSET:
            field_dict["requery_sql"] = requery_sql
        if requery_table_name is not UNSET:
            field_dict["requery_table_name"] = requery_table_name
        if result is not UNSET:
            field_dict["result"] = result
        if stream_stats is not UNSET:
            field_dict["stream_stats"] = stream_stats
        if summary is not UNSET:
            field_dict["summary"] = summary
        if used_keys is not UNSET:
            field_dict["used_keys"] = used_keys

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_stream_job_line_column_name_mapping import QueryStreamJobLineColumnNameMapping
        from ..models.query_stream_job_line_stream_stats import QueryStreamJobLineStreamStats
        from ..models.query_stream_job_line_used_keys import QueryStreamJobLineUsedKeys

        d = dict(src_dict)
        job_id = d.pop("job_id")

        status = d.pop("status")

        cache_metadata = d.pop("cache_metadata", UNSET)

        client_result_id = d.pop("client_result_id", UNSET)

        _column_name_mapping = d.pop("column_name_mapping", UNSET)
        column_name_mapping: QueryStreamJobLineColumnNameMapping | Unset
        if isinstance(_column_name_mapping, Unset):
            column_name_mapping = UNSET
        else:
            column_name_mapping = QueryStreamJobLineColumnNameMapping.from_dict(_column_name_mapping)

        error = d.pop("error", UNSET)

        error_message = d.pop("error_message", UNSET)

        error_type = d.pop("error_type", UNSET)

        kill_reason = d.pop("kill_reason", UNSET)

        query = d.pop("query", UNSET)

        requery_fallback_sql = d.pop("requery_fallback_sql", UNSET)

        requery_sql = d.pop("requery_sql", UNSET)

        requery_table_name = d.pop("requery_table_name", UNSET)

        result = d.pop("result", UNSET)

        _stream_stats = d.pop("stream_stats", UNSET)
        stream_stats: QueryStreamJobLineStreamStats | Unset
        if isinstance(_stream_stats, Unset):
            stream_stats = UNSET
        else:
            stream_stats = QueryStreamJobLineStreamStats.from_dict(_stream_stats)

        summary = d.pop("summary", UNSET)

        _used_keys = d.pop("used_keys", UNSET)
        used_keys: QueryStreamJobLineUsedKeys | Unset
        if isinstance(_used_keys, Unset):
            used_keys = UNSET
        else:
            used_keys = QueryStreamJobLineUsedKeys.from_dict(_used_keys)

        query_stream_job_line = cls(
            job_id=job_id,
            status=status,
            cache_metadata=cache_metadata,
            client_result_id=client_result_id,
            column_name_mapping=column_name_mapping,
            error=error,
            error_message=error_message,
            error_type=error_type,
            kill_reason=kill_reason,
            query=query,
            requery_fallback_sql=requery_fallback_sql,
            requery_sql=requery_sql,
            requery_table_name=requery_table_name,
            result=result,
            stream_stats=stream_stats,
            summary=summary,
            used_keys=used_keys,
        )

        query_stream_job_line.additional_properties = d
        return query_stream_job_line

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
