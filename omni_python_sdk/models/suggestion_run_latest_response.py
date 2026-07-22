from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.suggestion_run_latest_response_run import SuggestionRunLatestResponseRun


T = TypeVar("T", bound="SuggestionRunLatestResponse")


@_attrs_define
class SuggestionRunLatestResponse:
    """
    Attributes:
        run (SuggestionRunLatestResponseRun):
    """

    run: SuggestionRunLatestResponseRun
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        run = self.run.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "run": run,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.suggestion_run_latest_response_run import SuggestionRunLatestResponseRun

        d = dict(src_dict)
        run = SuggestionRunLatestResponseRun.from_dict(d.pop("run"))

        suggestion_run_latest_response = cls(
            run=run,
        )

        suggestion_run_latest_response.additional_properties = d
        return suggestion_run_latest_response

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
