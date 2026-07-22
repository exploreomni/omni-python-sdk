from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="QueryStreamJobsSubmittedLineJobsSubmitted")


@_attrs_define
class QueryStreamJobsSubmittedLineJobsSubmitted:
    """Map of submitted job ID to the client result ID for that job (null when the job has no client result ID). Job IDs
    are the keys to poll via /api/v1/query/wait.

    """

    additional_properties: dict[str, None | UUID] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, UUID):
                field_dict[prop_name] = str(prop)
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query_stream_jobs_submitted_line_jobs_submitted = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> None | UUID:
                if data is None:
                    return data
                try:
                    if not isinstance(data, str):
                        raise TypeError()
                    additional_property_type_0 = UUID(data)

                    return additional_property_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(None | UUID, data)

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        query_stream_jobs_submitted_line_jobs_submitted.additional_properties = additional_properties
        return query_stream_jobs_submitted_line_jobs_submitted

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> None | UUID:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: None | UUID) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
