from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ModelsContentValidatorReplaceResponse")


@_attrs_define
class ModelsContentValidatorReplaceResponse:
    """
    Attributes:
        replaced_dashboard_filters_count (int): Number of dashboard filters replaced
        replaced_documents_count (int): Number of documents modified
        replaced_input_column_keys_count (int): Number of input columns whose key references were replaced
        replaced_queries_count (int): Number of queries replaced
        replaced_workbook_models_count (int): Number of workbook models replaced
        skipped_pr_required_count (int): Number of documents skipped due to pull request requirements
    """

    replaced_dashboard_filters_count: int
    replaced_documents_count: int
    replaced_input_column_keys_count: int
    replaced_queries_count: int
    replaced_workbook_models_count: int
    skipped_pr_required_count: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        replaced_dashboard_filters_count = self.replaced_dashboard_filters_count

        replaced_documents_count = self.replaced_documents_count

        replaced_input_column_keys_count = self.replaced_input_column_keys_count

        replaced_queries_count = self.replaced_queries_count

        replaced_workbook_models_count = self.replaced_workbook_models_count

        skipped_pr_required_count = self.skipped_pr_required_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "replaced_dashboard_filters_count": replaced_dashboard_filters_count,
                "replaced_documents_count": replaced_documents_count,
                "replaced_input_column_keys_count": replaced_input_column_keys_count,
                "replaced_queries_count": replaced_queries_count,
                "replaced_workbook_models_count": replaced_workbook_models_count,
                "skipped_pr_required_count": skipped_pr_required_count,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        replaced_dashboard_filters_count = d.pop("replaced_dashboard_filters_count")

        replaced_documents_count = d.pop("replaced_documents_count")

        replaced_input_column_keys_count = d.pop("replaced_input_column_keys_count")

        replaced_queries_count = d.pop("replaced_queries_count")

        replaced_workbook_models_count = d.pop("replaced_workbook_models_count")

        skipped_pr_required_count = d.pop("skipped_pr_required_count")

        models_content_validator_replace_response = cls(
            replaced_dashboard_filters_count=replaced_dashboard_filters_count,
            replaced_documents_count=replaced_documents_count,
            replaced_input_column_keys_count=replaced_input_column_keys_count,
            replaced_queries_count=replaced_queries_count,
            replaced_workbook_models_count=replaced_workbook_models_count,
            skipped_pr_required_count=skipped_pr_required_count,
        )

        models_content_validator_replace_response.additional_properties = d
        return models_content_validator_replace_response

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
