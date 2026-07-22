from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.documents_create_body_query_presentations_item import DocumentsCreateBodyQueryPresentationsItem


T = TypeVar("T", bound="DocumentsCreateBody")


@_attrs_define
class DocumentsCreateBody:
    """
    Attributes:
        model_id (str): Shared model ID to base the document on
        name (str): Document name
        branch_id (UUID | Unset): Optional branch ID to associate the document with a model branch Example:
            123e4567-e89b-12d3-a456-426614174001.
        description (None | str | Unset): Document description
        facet_filters (bool | Unset): Enable facet filters on the dashboard
        filter_config (Any | Unset): Dashboard filter configuration
        filter_order (list[str] | Unset): Order of filters in the dashboard
        identifier (str | Unset): Optional document identifier. If omitted, an identifier is auto-generated. Must be
            unique within the organization.
        metadata (Any | Unset): Dashboard metadata
        metadata_version (str | Unset): Dashboard metadata version (required when metadata is provided)
        query_presentations (list[DocumentsCreateBodyQueryPresentationsItem] | Unset): Query presentations for the
            document
    """

    model_id: str
    name: str
    branch_id: UUID | Unset = UNSET
    description: None | str | Unset = UNSET
    facet_filters: bool | Unset = UNSET
    filter_config: Any | Unset = UNSET
    filter_order: list[str] | Unset = UNSET
    identifier: str | Unset = UNSET
    metadata: Any | Unset = UNSET
    metadata_version: str | Unset = UNSET
    query_presentations: list[DocumentsCreateBodyQueryPresentationsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = self.model_id

        name = self.name

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        facet_filters = self.facet_filters

        filter_config = self.filter_config

        filter_order: list[str] | Unset = UNSET
        if not isinstance(self.filter_order, Unset):
            filter_order = self.filter_order

        identifier = self.identifier

        metadata = self.metadata

        metadata_version = self.metadata_version

        query_presentations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.query_presentations, Unset):
            query_presentations = []
            for query_presentations_item_data in self.query_presentations:
                query_presentations_item = query_presentations_item_data.to_dict()
                query_presentations.append(query_presentations_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "modelId": model_id,
                "name": name,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if description is not UNSET:
            field_dict["description"] = description
        if facet_filters is not UNSET:
            field_dict["facetFilters"] = facet_filters
        if filter_config is not UNSET:
            field_dict["filterConfig"] = filter_config
        if filter_order is not UNSET:
            field_dict["filterOrder"] = filter_order
        if identifier is not UNSET:
            field_dict["identifier"] = identifier
        if metadata is not UNSET:
            field_dict["metadata"] = metadata
        if metadata_version is not UNSET:
            field_dict["metadataVersion"] = metadata_version
        if query_presentations is not UNSET:
            field_dict["queryPresentations"] = query_presentations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.documents_create_body_query_presentations_item import DocumentsCreateBodyQueryPresentationsItem

        d = dict(src_dict)
        model_id = d.pop("modelId")

        name = d.pop("name")

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        facet_filters = d.pop("facetFilters", UNSET)

        filter_config = d.pop("filterConfig", UNSET)

        filter_order = cast(list[str], d.pop("filterOrder", UNSET))

        identifier = d.pop("identifier", UNSET)

        metadata = d.pop("metadata", UNSET)

        metadata_version = d.pop("metadataVersion", UNSET)

        _query_presentations = d.pop("queryPresentations", UNSET)
        query_presentations: list[DocumentsCreateBodyQueryPresentationsItem] | Unset = UNSET
        if _query_presentations is not UNSET:
            query_presentations = []
            for query_presentations_item_data in _query_presentations:
                query_presentations_item = DocumentsCreateBodyQueryPresentationsItem.from_dict(
                    query_presentations_item_data
                )

                query_presentations.append(query_presentations_item)

        documents_create_body = cls(
            model_id=model_id,
            name=name,
            branch_id=branch_id,
            description=description,
            facet_filters=facet_filters,
            filter_config=filter_config,
            filter_order=filter_order,
            identifier=identifier,
            metadata=metadata,
            metadata_version=metadata_version,
            query_presentations=query_presentations,
        )

        documents_create_body.additional_properties = d
        return documents_create_body

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
