from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.ai_search_omni_docs_response_sources_item import AiSearchOmniDocsResponseSourcesItem


T = TypeVar("T", bound="AiSearchOmniDocsResponse")


@_attrs_define
class AiSearchOmniDocsResponse:
    """
    Attributes:
        answer (str): A synthesized answer to the question, based on the Omni documentation. Example: To create a
            dashboard filter, navigate to your dashboard and click the "Add Filter" button....
        sources (list[AiSearchOmniDocsResponseSourcesItem]): List of documentation pages that were used to synthesize
            the answer.
    """

    answer: str
    sources: list[AiSearchOmniDocsResponseSourcesItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        answer = self.answer

        sources = []
        for sources_item_data in self.sources:
            sources_item = sources_item_data.to_dict()
            sources.append(sources_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "answer": answer,
                "sources": sources,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_search_omni_docs_response_sources_item import AiSearchOmniDocsResponseSourcesItem

        d = dict(src_dict)
        answer = d.pop("answer")

        sources = []
        _sources = d.pop("sources")
        for sources_item_data in _sources:
            sources_item = AiSearchOmniDocsResponseSourcesItem.from_dict(sources_item_data)

            sources.append(sources_item)

        ai_search_omni_docs_response = cls(
            answer=answer,
            sources=sources,
        )

        ai_search_omni_docs_response.additional_properties = d
        return ai_search_omni_docs_response

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
