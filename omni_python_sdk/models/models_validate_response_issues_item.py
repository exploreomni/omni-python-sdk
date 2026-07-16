from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.models_validate_response_issues_item_severity import (
    ModelsValidateResponseIssuesItemSeverity,
    check_models_validate_response_issues_item_severity,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsValidateResponseIssuesItem")


@_attrs_define
class ModelsValidateResponseIssuesItem:
    """
    Attributes:
        message (str): Validation issue message
        severity (ModelsValidateResponseIssuesItemSeverity): Issue severity
        field (str | Unset): Field name with the issue
        view (str | Unset): View name with the issue
    """

    message: str
    severity: ModelsValidateResponseIssuesItemSeverity
    field: str | Unset = UNSET
    view: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        severity: str = self.severity

        field = self.field

        view = self.view

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
                "severity": severity,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if view is not UNSET:
            field_dict["view"] = view

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message")

        severity = check_models_validate_response_issues_item_severity(d.pop("severity"))

        field = d.pop("field", UNSET)

        view = d.pop("view", UNSET)

        models_validate_response_issues_item = cls(
            message=message,
            severity=severity,
            field=field,
            view=view,
        )

        models_validate_response_issues_item.additional_properties = d
        return models_validate_response_issues_item

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
