from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.models_refresh_response_status import ModelsRefreshResponseStatus, check_models_refresh_response_status

T = TypeVar("T", bound="ModelsRefreshResponse")


@_attrs_define
class ModelsRefreshResponse:
    """
    Attributes:
        job_id (str): Job ID for the refresh operation
        model_id (str): Model ID being refreshed
        status (ModelsRefreshResponseStatus): Current status of the refresh
    """

    job_id: str
    model_id: str
    status: ModelsRefreshResponseStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        model_id = self.model_id

        status: str = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "jobId": job_id,
                "modelId": model_id,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        job_id = d.pop("jobId")

        model_id = d.pop("modelId")

        status = check_models_refresh_response_status(d.pop("status"))

        models_refresh_response = cls(
            job_id=job_id,
            model_id=model_id,
            status=status,
        )

        models_refresh_response.additional_properties = d
        return models_refresh_response

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
