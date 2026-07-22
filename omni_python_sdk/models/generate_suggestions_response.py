from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.generate_suggestions_response_status import (
    GenerateSuggestionsResponseStatus,
    check_generate_suggestions_response_status,
)

T = TypeVar("T", bound="GenerateSuggestionsResponse")


@_attrs_define
class GenerateSuggestionsResponse:
    """
    Attributes:
        run_id (UUID): The id of the created generation run. Poll `GET /suggestions/runs/{runId}` for status.
        status (GenerateSuggestionsResponseStatus): The generation run was enqueued. Generation runs asynchronously.
    """

    run_id: UUID
    status: GenerateSuggestionsResponseStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        run_id = str(self.run_id)

        status: str = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "runId": run_id,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        run_id = UUID(d.pop("runId"))

        status = check_generate_suggestions_response_status(d.pop("status"))

        generate_suggestions_response = cls(
            run_id=run_id,
            status=status,
        )

        generate_suggestions_response.additional_properties = d
        return generate_suggestions_response

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
