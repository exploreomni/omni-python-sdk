from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.models_update_field_body_filters import ModelsUpdateFieldBodyFilters
    from ..models.models_update_field_body_group_filters_item import ModelsUpdateFieldBodyGroupFiltersItem


T = TypeVar("T", bound="ModelsUpdateFieldBody")


@_attrs_define
class ModelsUpdateFieldBody:
    """
    Attributes:
        ai_context (str | Unset): AI context for the field
        all_values (list[str] | Unset): Deprecated: use sampleValues instead
        bin_boundaries (list[float] | Unset): Bin boundaries for binned fields
        bin_labels (list[str] | Unset): Labels for bins
        description (str | Unset): Field description
        drill_fields (list[str] | Unset): Drill-down fields
        else_value (str | Unset): Else value for grouped fields
        filters (ModelsUpdateFieldBodyFilters | Unset): Filters for the field
        format_ (str | Unset): Field format
        group_filters (list[ModelsUpdateFieldBodyGroupFiltersItem] | Unset): Group filters
        group_label (str | Unset): Group label
        group_names (list[str] | Unset): Group names
        hidden (bool | Unset): Whether the field is hidden
        ignored (bool | Unset): Whether the field is ignored
        is_calc (bool | Unset): Whether this is a calculation field
        label (str | Unset): Field label
        new_field_name (str | Unset): New field name (for rename)
        new_view_name (str | Unset): New view name (for move)
        sample_values (list[str] | Unset): Sample values for the field
        sql (str | Unset): SQL expression for the field
        synonyms (list[str] | Unset): Synonyms for the field
        tags (list[str] | Unset): Tags for the field
        topic_context (str | Unset): Topic context for the field
    """

    ai_context: str | Unset = UNSET
    all_values: list[str] | Unset = UNSET
    bin_boundaries: list[float] | Unset = UNSET
    bin_labels: list[str] | Unset = UNSET
    description: str | Unset = UNSET
    drill_fields: list[str] | Unset = UNSET
    else_value: str | Unset = UNSET
    filters: ModelsUpdateFieldBodyFilters | Unset = UNSET
    format_: str | Unset = UNSET
    group_filters: list[ModelsUpdateFieldBodyGroupFiltersItem] | Unset = UNSET
    group_label: str | Unset = UNSET
    group_names: list[str] | Unset = UNSET
    hidden: bool | Unset = UNSET
    ignored: bool | Unset = UNSET
    is_calc: bool | Unset = UNSET
    label: str | Unset = UNSET
    new_field_name: str | Unset = UNSET
    new_view_name: str | Unset = UNSET
    sample_values: list[str] | Unset = UNSET
    sql: str | Unset = UNSET
    synonyms: list[str] | Unset = UNSET
    tags: list[str] | Unset = UNSET
    topic_context: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ai_context = self.ai_context

        all_values: list[str] | Unset = UNSET
        if not isinstance(self.all_values, Unset):
            all_values = self.all_values

        bin_boundaries: list[float] | Unset = UNSET
        if not isinstance(self.bin_boundaries, Unset):
            bin_boundaries = self.bin_boundaries

        bin_labels: list[str] | Unset = UNSET
        if not isinstance(self.bin_labels, Unset):
            bin_labels = self.bin_labels

        description = self.description

        drill_fields: list[str] | Unset = UNSET
        if not isinstance(self.drill_fields, Unset):
            drill_fields = self.drill_fields

        else_value = self.else_value

        filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filters, Unset):
            filters = self.filters.to_dict()

        format_ = self.format_

        group_filters: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.group_filters, Unset):
            group_filters = []
            for group_filters_item_data in self.group_filters:
                group_filters_item = group_filters_item_data.to_dict()
                group_filters.append(group_filters_item)

        group_label = self.group_label

        group_names: list[str] | Unset = UNSET
        if not isinstance(self.group_names, Unset):
            group_names = self.group_names

        hidden = self.hidden

        ignored = self.ignored

        is_calc = self.is_calc

        label = self.label

        new_field_name = self.new_field_name

        new_view_name = self.new_view_name

        sample_values: list[str] | Unset = UNSET
        if not isinstance(self.sample_values, Unset):
            sample_values = self.sample_values

        sql = self.sql

        synonyms: list[str] | Unset = UNSET
        if not isinstance(self.synonyms, Unset):
            synonyms = self.synonyms

        tags: list[str] | Unset = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        topic_context = self.topic_context

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if ai_context is not UNSET:
            field_dict["aiContext"] = ai_context
        if all_values is not UNSET:
            field_dict["allValues"] = all_values
        if bin_boundaries is not UNSET:
            field_dict["binBoundaries"] = bin_boundaries
        if bin_labels is not UNSET:
            field_dict["binLabels"] = bin_labels
        if description is not UNSET:
            field_dict["description"] = description
        if drill_fields is not UNSET:
            field_dict["drillFields"] = drill_fields
        if else_value is not UNSET:
            field_dict["elseValue"] = else_value
        if filters is not UNSET:
            field_dict["filters"] = filters
        if format_ is not UNSET:
            field_dict["format"] = format_
        if group_filters is not UNSET:
            field_dict["groupFilters"] = group_filters
        if group_label is not UNSET:
            field_dict["groupLabel"] = group_label
        if group_names is not UNSET:
            field_dict["groupNames"] = group_names
        if hidden is not UNSET:
            field_dict["hidden"] = hidden
        if ignored is not UNSET:
            field_dict["ignored"] = ignored
        if is_calc is not UNSET:
            field_dict["isCalc"] = is_calc
        if label is not UNSET:
            field_dict["label"] = label
        if new_field_name is not UNSET:
            field_dict["newFieldName"] = new_field_name
        if new_view_name is not UNSET:
            field_dict["newViewName"] = new_view_name
        if sample_values is not UNSET:
            field_dict["sampleValues"] = sample_values
        if sql is not UNSET:
            field_dict["sql"] = sql
        if synonyms is not UNSET:
            field_dict["synonyms"] = synonyms
        if tags is not UNSET:
            field_dict["tags"] = tags
        if topic_context is not UNSET:
            field_dict["topicContext"] = topic_context

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_update_field_body_filters import ModelsUpdateFieldBodyFilters
        from ..models.models_update_field_body_group_filters_item import ModelsUpdateFieldBodyGroupFiltersItem

        d = dict(src_dict)
        ai_context = d.pop("aiContext", UNSET)

        all_values = cast(list[str], d.pop("allValues", UNSET))

        bin_boundaries = cast(list[float], d.pop("binBoundaries", UNSET))

        bin_labels = cast(list[str], d.pop("binLabels", UNSET))

        description = d.pop("description", UNSET)

        drill_fields = cast(list[str], d.pop("drillFields", UNSET))

        else_value = d.pop("elseValue", UNSET)

        _filters = d.pop("filters", UNSET)
        filters: ModelsUpdateFieldBodyFilters | Unset
        if isinstance(_filters, Unset):
            filters = UNSET
        else:
            filters = ModelsUpdateFieldBodyFilters.from_dict(_filters)

        format_ = d.pop("format", UNSET)

        _group_filters = d.pop("groupFilters", UNSET)
        group_filters: list[ModelsUpdateFieldBodyGroupFiltersItem] | Unset = UNSET
        if _group_filters is not UNSET:
            group_filters = []
            for group_filters_item_data in _group_filters:
                group_filters_item = ModelsUpdateFieldBodyGroupFiltersItem.from_dict(group_filters_item_data)

                group_filters.append(group_filters_item)

        group_label = d.pop("groupLabel", UNSET)

        group_names = cast(list[str], d.pop("groupNames", UNSET))

        hidden = d.pop("hidden", UNSET)

        ignored = d.pop("ignored", UNSET)

        is_calc = d.pop("isCalc", UNSET)

        label = d.pop("label", UNSET)

        new_field_name = d.pop("newFieldName", UNSET)

        new_view_name = d.pop("newViewName", UNSET)

        sample_values = cast(list[str], d.pop("sampleValues", UNSET))

        sql = d.pop("sql", UNSET)

        synonyms = cast(list[str], d.pop("synonyms", UNSET))

        tags = cast(list[str], d.pop("tags", UNSET))

        topic_context = d.pop("topicContext", UNSET)

        models_update_field_body = cls(
            ai_context=ai_context,
            all_values=all_values,
            bin_boundaries=bin_boundaries,
            bin_labels=bin_labels,
            description=description,
            drill_fields=drill_fields,
            else_value=else_value,
            filters=filters,
            format_=format_,
            group_filters=group_filters,
            group_label=group_label,
            group_names=group_names,
            hidden=hidden,
            ignored=ignored,
            is_calc=is_calc,
            label=label,
            new_field_name=new_field_name,
            new_view_name=new_view_name,
            sample_values=sample_values,
            sql=sql,
            synonyms=synonyms,
            tags=tags,
            topic_context=topic_context,
        )

        models_update_field_body.additional_properties = d
        return models_update_field_body

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
