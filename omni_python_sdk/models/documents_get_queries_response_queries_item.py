from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsGetQueriesResponseQueriesItem")


@_attrs_define
class DocumentsGetQueriesResponseQueriesItem:
    """
    Attributes:
        id (str): Query presentation ID
        name (str): Query presentation name
        query_identifier_map_key (str): Key in the query identifier map
        url (str): URL to view this specific query/sheet in the workbook. Example: https://org.omni.co/w/abc123?key=1.
        query (Any | Unset): Query JSON definition
    """

    id: str
    name: str
    query_identifier_map_key: str
    url: str
    query: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        query_identifier_map_key = self.query_identifier_map_key

        url = self.url

        query = self.query

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "queryIdentifierMapKey": query_identifier_map_key,
                "url": url,
            }
        )
        if query is not UNSET:
            field_dict["query"] = query

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        query_identifier_map_key = d.pop("queryIdentifierMapKey")

        url = d.pop("url")

        query = d.pop("query", UNSET)

        documents_get_queries_response_queries_item = cls(
            id=id,
            name=name,
            query_identifier_map_key=query_identifier_map_key,
            url=url,
            query=query,
        )

        documents_get_queries_response_queries_item.additional_properties = d
        return documents_get_queries_response_queries_item

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
