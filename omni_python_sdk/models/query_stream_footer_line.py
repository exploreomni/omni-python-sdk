from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_stream_footer_line_timed_out import (
    QueryStreamFooterLineTimedOut,
    check_query_stream_footer_line_timed_out,
)

T = TypeVar("T", bound="QueryStreamFooterLine")


@_attrs_define
class QueryStreamFooterLine:
    """Last line of the stream.

    Attributes:
        remaining_job_ids (list[str]): Job IDs that had not completed when the wait window elapsed. Poll
            /api/v1/query/wait with these IDs until the list is empty.
        timed_out (QueryStreamFooterLineTimedOut): Whether the wait window elapsed before every job completed. Note: a
            string ("true"/"false"), not a boolean. Example: false.
    """

    remaining_job_ids: list[str]
    timed_out: QueryStreamFooterLineTimedOut
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        remaining_job_ids = self.remaining_job_ids

        timed_out: str = self.timed_out

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "remaining_job_ids": remaining_job_ids,
                "timed_out": timed_out,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        remaining_job_ids = cast(list[str], d.pop("remaining_job_ids"))

        timed_out = check_query_stream_footer_line_timed_out(d.pop("timed_out"))

        query_stream_footer_line = cls(
            remaining_job_ids=remaining_job_ids,
            timed_out=timed_out,
        )

        query_stream_footer_line.additional_properties = d
        return query_stream_footer_line

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
