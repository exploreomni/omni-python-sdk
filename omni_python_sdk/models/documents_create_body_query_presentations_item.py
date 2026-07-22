from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.api_vis_config import ApiVisConfig
    from ..models.documents_create_body_query_presentations_item_query import (
        DocumentsCreateBodyQueryPresentationsItemQuery,
    )


T = TypeVar("T", bound="DocumentsCreateBodyQueryPresentationsItem")


@_attrs_define
class DocumentsCreateBodyQueryPresentationsItem:
    """
    Attributes:
        name (str): Query presentation name
        query (DocumentsCreateBodyQueryPresentationsItemQuery): Query definition
        ai_config (Any | Unset): AI configuration
        chart_type (None | str | Unset): Chart type
        description (str | Unset): Query presentation description
        prefers_chart (bool | Unset): Whether to prefer chart view
        result_config (Any | Unset): Result configuration
        sub_title (str | Unset): Subtitle
        topic_name (None | str | Unset): Topic name. Omit or pass null for raw-SQL tiles or any tile with no semantic
            topic.
        vis_config (ApiVisConfig | Unset): Visualization configuration (Not statically modeled; use plain dicts.)
    """

    name: str
    query: DocumentsCreateBodyQueryPresentationsItemQuery
    ai_config: Any | Unset = UNSET
    chart_type: None | str | Unset = UNSET
    description: str | Unset = UNSET
    prefers_chart: bool | Unset = UNSET
    result_config: Any | Unset = UNSET
    sub_title: str | Unset = UNSET
    topic_name: None | str | Unset = UNSET
    vis_config: ApiVisConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        query = self.query.to_dict()

        ai_config = self.ai_config

        chart_type: None | str | Unset
        if isinstance(self.chart_type, Unset):
            chart_type = UNSET
        else:
            chart_type = self.chart_type

        description = self.description

        prefers_chart = self.prefers_chart

        result_config = self.result_config

        sub_title = self.sub_title

        topic_name: None | str | Unset
        if isinstance(self.topic_name, Unset):
            topic_name = UNSET
        else:
            topic_name = self.topic_name

        vis_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vis_config, Unset):
            vis_config = self.vis_config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "query": query,
            }
        )
        if ai_config is not UNSET:
            field_dict["aiConfig"] = ai_config
        if chart_type is not UNSET:
            field_dict["chartType"] = chart_type
        if description is not UNSET:
            field_dict["description"] = description
        if prefers_chart is not UNSET:
            field_dict["prefersChart"] = prefers_chart
        if result_config is not UNSET:
            field_dict["resultConfig"] = result_config
        if sub_title is not UNSET:
            field_dict["subTitle"] = sub_title
        if topic_name is not UNSET:
            field_dict["topicName"] = topic_name
        if vis_config is not UNSET:
            field_dict["visConfig"] = vis_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.api_vis_config import ApiVisConfig
        from ..models.documents_create_body_query_presentations_item_query import (
            DocumentsCreateBodyQueryPresentationsItemQuery,
        )

        d = dict(src_dict)
        name = d.pop("name")

        query = DocumentsCreateBodyQueryPresentationsItemQuery.from_dict(d.pop("query"))

        ai_config = d.pop("aiConfig", UNSET)

        def _parse_chart_type(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        chart_type = _parse_chart_type(d.pop("chartType", UNSET))

        description = d.pop("description", UNSET)

        prefers_chart = d.pop("prefersChart", UNSET)

        result_config = d.pop("resultConfig", UNSET)

        sub_title = d.pop("subTitle", UNSET)

        def _parse_topic_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        topic_name = _parse_topic_name(d.pop("topicName", UNSET))

        _vis_config = d.pop("visConfig", UNSET)
        vis_config: ApiVisConfig | Unset
        if isinstance(_vis_config, Unset):
            vis_config = UNSET
        else:
            vis_config = ApiVisConfig.from_dict(_vis_config)

        documents_create_body_query_presentations_item = cls(
            name=name,
            query=query,
            ai_config=ai_config,
            chart_type=chart_type,
            description=description,
            prefers_chart=prefers_chart,
            result_config=result_config,
            sub_title=sub_title,
            topic_name=topic_name,
            vis_config=vis_config,
        )

        documents_create_body_query_presentations_item.additional_properties = d
        return documents_create_body_query_presentations_item

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
