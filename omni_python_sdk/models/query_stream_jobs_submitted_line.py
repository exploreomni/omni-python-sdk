from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.query_stream_jobs_submitted_line_jobs_submitted import QueryStreamJobsSubmittedLineJobsSubmitted


T = TypeVar("T", bound="QueryStreamJobsSubmittedLine")


@_attrs_define
class QueryStreamJobsSubmittedLine:
    """First line of the query/run stream: the jobs accepted for execution.

    Attributes:
        jobs_submitted (QueryStreamJobsSubmittedLineJobsSubmitted): Map of submitted job ID to the client result ID for
            that job (null when the job has no client result ID). Job IDs are the keys to poll via /api/v1/query/wait.
    """

    jobs_submitted: QueryStreamJobsSubmittedLineJobsSubmitted
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        jobs_submitted = self.jobs_submitted.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "jobs_submitted": jobs_submitted,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_stream_jobs_submitted_line_jobs_submitted import QueryStreamJobsSubmittedLineJobsSubmitted

        d = dict(src_dict)
        jobs_submitted = QueryStreamJobsSubmittedLineJobsSubmitted.from_dict(d.pop("jobs_submitted"))

        query_stream_jobs_submitted_line = cls(
            jobs_submitted=jobs_submitted,
        )

        query_stream_jobs_submitted_line.additional_properties = d
        return query_stream_jobs_submitted_line

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
