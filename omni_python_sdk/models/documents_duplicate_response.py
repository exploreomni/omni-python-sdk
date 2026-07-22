from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentsDuplicateResponse")


@_attrs_define
class DocumentsDuplicateResponse:
    """
    Attributes:
        dashboard_id (str): New dashboard ID
        identifier (str): New document identifier
        name (str): Document name
        workbook_id (str): New workbook ID
    """

    dashboard_id: str
    identifier: str
    name: str
    workbook_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dashboard_id = self.dashboard_id

        identifier = self.identifier

        name = self.name

        workbook_id = self.workbook_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "dashboardId": dashboard_id,
                "identifier": identifier,
                "name": name,
                "workbookId": workbook_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dashboard_id = d.pop("dashboardId")

        identifier = d.pop("identifier")

        name = d.pop("name")

        workbook_id = d.pop("workbookId")

        documents_duplicate_response = cls(
            dashboard_id=dashboard_id,
            identifier=identifier,
            name=name,
            workbook_id=workbook_id,
        )

        documents_duplicate_response.additional_properties = d
        return documents_duplicate_response

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
