from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentsCreateBodyQueryPresentationsItemQuery")


@_attrs_define
class DocumentsCreateBodyQueryPresentationsItemQuery:
    """Query definition

    Attributes:
        fields (list[str]): Query fields
        table (str): Query table/topic
    """

    fields: list[str]
    table: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        table = self.table

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "fields": fields,
                "table": table,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        table = d.pop("table")

        documents_create_body_query_presentations_item_query = cls(
            fields=fields,
            table=table,
        )

        documents_create_body_query_presentations_item_query.additional_properties = d
        return documents_create_body_query_presentations_item_query

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
