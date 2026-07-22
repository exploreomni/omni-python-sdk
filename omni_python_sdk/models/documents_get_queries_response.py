from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.documents_get_queries_response_queries_item import DocumentsGetQueriesResponseQueriesItem


T = TypeVar("T", bound="DocumentsGetQueriesResponse")


@_attrs_define
class DocumentsGetQueriesResponse:
    """
    Attributes:
        queries (list[DocumentsGetQueriesResponseQueriesItem]): List of queries in the document
    """

    queries: list[DocumentsGetQueriesResponseQueriesItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        queries = []
        for queries_item_data in self.queries:
            queries_item = queries_item_data.to_dict()
            queries.append(queries_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "queries": queries,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.documents_get_queries_response_queries_item import DocumentsGetQueriesResponseQueriesItem

        d = dict(src_dict)
        queries = []
        _queries = d.pop("queries")
        for queries_item_data in _queries:
            queries_item = DocumentsGetQueriesResponseQueriesItem.from_dict(queries_item_data)

            queries.append(queries_item)

        documents_get_queries_response = cls(
            queries=queries,
        )

        documents_get_queries_response.additional_properties = d
        return documents_get_queries_response

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
