from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsGetResponse")


@_attrs_define
class DocumentsGetResponse:
    """
    Attributes:
        facet_filters (bool): Whether facet filters are enabled
        filter_order (list[str]): Order of filters
        model_id (str): Model ID
        name (str): Document name
        query_presentations (list[Any]): Query presentations
        refresh_interval (float | None): Auto-refresh interval in seconds
        description (None | str | Unset): Document description
        document_metadata (Any | Unset): Document metadata
        filter_config (Any | Unset): Dashboard filter configuration
    """

    facet_filters: bool
    filter_order: list[str]
    model_id: str
    name: str
    query_presentations: list[Any]
    refresh_interval: float | None
    description: None | str | Unset = UNSET
    document_metadata: Any | Unset = UNSET
    filter_config: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        facet_filters = self.facet_filters

        filter_order = self.filter_order

        model_id = self.model_id

        name = self.name

        query_presentations = self.query_presentations

        refresh_interval: float | None
        refresh_interval = self.refresh_interval

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        document_metadata = self.document_metadata

        filter_config = self.filter_config

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "facetFilters": facet_filters,
                "filterOrder": filter_order,
                "modelId": model_id,
                "name": name,
                "queryPresentations": query_presentations,
                "refreshInterval": refresh_interval,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if document_metadata is not UNSET:
            field_dict["documentMetadata"] = document_metadata
        if filter_config is not UNSET:
            field_dict["filterConfig"] = filter_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        facet_filters = d.pop("facetFilters")

        filter_order = cast(list[str], d.pop("filterOrder"))

        model_id = d.pop("modelId")

        name = d.pop("name")

        query_presentations = cast(list[Any], d.pop("queryPresentations"))

        def _parse_refresh_interval(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        refresh_interval = _parse_refresh_interval(d.pop("refreshInterval"))

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        document_metadata = d.pop("documentMetadata", UNSET)

        filter_config = d.pop("filterConfig", UNSET)

        documents_get_response = cls(
            facet_filters=facet_filters,
            filter_order=filter_order,
            model_id=model_id,
            name=name,
            query_presentations=query_presentations,
            refresh_interval=refresh_interval,
            description=description,
            document_metadata=document_metadata,
            filter_config=filter_config,
        )

        documents_get_response.additional_properties = d
        return documents_get_response

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
