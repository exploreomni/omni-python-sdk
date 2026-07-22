from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.models_create_field_body_aggregate_type import (
    ModelsCreateFieldBodyAggregateType,
    check_models_create_field_body_aggregate_type,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsCreateFieldBody")


@_attrs_define
class ModelsCreateFieldBody:
    """
    Attributes:
        field_name (str): Field name Example: total_revenue.
        view_name (str): View to add the field to Example: orders.
        aggregate_type (ModelsCreateFieldBodyAggregateType | Unset): Aggregate type for measures. Setting this property
            promotes the field to a measure (written under `measures:`); omit it to create a dimension (written under
            `dimensions:`). Values must be uppercase canonical names. Example: SUM.
        ai_context (str | Unset): AI context for the field
        description (str | Unset): Field description
        format_ (str | Unset): Field format
        hidden (bool | Unset): Whether the field is hidden
        label (str | Unset): Field label
        sql (str | Unset): SQL expression for the field
        tags (list[str] | Unset): Tags for the field
        topic_context (str | Unset): Topic context for topic-scoped fields
    """

    field_name: str
    view_name: str
    aggregate_type: ModelsCreateFieldBodyAggregateType | Unset = UNSET
    ai_context: str | Unset = UNSET
    description: str | Unset = UNSET
    format_: str | Unset = UNSET
    hidden: bool | Unset = UNSET
    label: str | Unset = UNSET
    sql: str | Unset = UNSET
    tags: list[str] | Unset = UNSET
    topic_context: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field_name = self.field_name

        view_name = self.view_name

        aggregate_type: str | Unset = UNSET
        if not isinstance(self.aggregate_type, Unset):
            aggregate_type = self.aggregate_type

        ai_context = self.ai_context

        description = self.description

        format_ = self.format_

        hidden = self.hidden

        label = self.label

        sql = self.sql

        tags: list[str] | Unset = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        topic_context = self.topic_context

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fieldName": field_name,
                "viewName": view_name,
            }
        )
        if aggregate_type is not UNSET:
            field_dict["aggregateType"] = aggregate_type
        if ai_context is not UNSET:
            field_dict["aiContext"] = ai_context
        if description is not UNSET:
            field_dict["description"] = description
        if format_ is not UNSET:
            field_dict["format"] = format_
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if label is not UNSET:
            field_dict["label"] = label
        if sql is not UNSET:
            field_dict["sql"] = sql
        if tags is not UNSET:
            field_dict["tags"] = tags
        if topic_context is not UNSET:
            field_dict["topicContext"] = topic_context

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field_name = d.pop("fieldName")

        view_name = d.pop("viewName")

        _aggregate_type = d.pop("aggregateType", UNSET)
        aggregate_type: ModelsCreateFieldBodyAggregateType | Unset
        if isinstance(_aggregate_type, Unset):
            aggregate_type = UNSET
        else:
            aggregate_type = check_models_create_field_body_aggregate_type(_aggregate_type)

        ai_context = d.pop("aiContext", UNSET)

        description = d.pop("description", UNSET)

        format_ = d.pop("format", UNSET)

        hidden = d.pop("hidden", UNSET)

        label = d.pop("label", UNSET)

        sql = d.pop("sql", UNSET)

        tags = cast(list[str], d.pop("tags", UNSET))

        topic_context = d.pop("topicContext", UNSET)

        models_create_field_body = cls(
            field_name=field_name,
            view_name=view_name,
            aggregate_type=aggregate_type,
            ai_context=ai_context,
            description=description,
            format_=format_,
            hidden=hidden,
            label=label,
            sql=sql,
            tags=tags,
            topic_context=topic_context,
        )

        return models_create_field_body
