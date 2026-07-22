from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.jobs_get_status_response_status import JobsGetStatusResponseStatus, check_jobs_get_status_response_status

T = TypeVar("T", bound="JobsGetStatusResponse")


@_attrs_define
class JobsGetStatusResponse:
    """
    Attributes:
        job_id (str): The job ID Example: 550e8400-e29b-41d4-a716-446655440000.
        job_type (str): The type of job (e.g., REFRESH_SCHEMA) Example: REFRESH_SCHEMA.
        status (JobsGetStatusResponseStatus): Current status of the job Example: COMPLETED.
    """

    job_id: str
    job_type: str
    status: JobsGetStatusResponseStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        job_type = self.job_type

        status: str = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_id": job_id,
                "job_type": job_type,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        job_id = d.pop("job_id")

        job_type = d.pop("job_type")

        status = check_jobs_get_status_response_status(d.pop("status"))

        jobs_get_status_response = cls(
            job_id=job_id,
            job_type=job_type,
            status=status,
        )

        jobs_get_status_response.additional_properties = d
        return jobs_get_status_response

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
