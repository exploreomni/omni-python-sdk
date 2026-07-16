from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiGenerateQueryBody")


@_attrs_define
class AiGenerateQueryBody:
    """
    Attributes:
        model_id (UUID): The UUID of the shared model to query against. Only shared models are supported. Example:
            770e8400-e29b-41d4-a716-446655440002.
        prompt (str): The natural language prompt describing the data you want to retrieve. Example: Show me total
            revenue by month for the last year.
        branch_id (UUID | Unset): Optional branch ID for the model. Must be a branch of the shared model specified by
            modelId. Example: 550e8400-e29b-41d4-a716-446655440000.
        current_topic_name (str | Unset): The name of the current topic to scope query generation. If not provided, AI
            will automatically select the best topic for your prompt. Example: order_items.
        query_all_views (bool | Unset): If true and the model has query_all_views_and_fields enabled, AI can query views
            not in any topic.
        run_query (bool | Unset): Whether to execute the generated query and return results. Defaults to true. Set to
            false to only generate the query definition without executing it. Example: True.
        user_id (UUID | Unset): User ID to execute the query as. Their permissions will be applied for row-level
            security. Only valid with organization-scoped API keys. Personal access tokens always act as the authenticated
            user. Example: 990e8400-e29b-41d4-a716-446655440004.
        workbook_url (bool | Unset): If true, creates a new workbook with the generated query and returns its URL.
            Useful for sharing results or further exploration.
    """

    model_id: UUID
    prompt: str
    branch_id: UUID | Unset = UNSET
    current_topic_name: str | Unset = UNSET
    query_all_views: bool | Unset = UNSET
    run_query: bool | Unset = UNSET
    user_id: UUID | Unset = UNSET
    workbook_url: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = str(self.model_id)

        prompt = self.prompt

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        current_topic_name = self.current_topic_name

        query_all_views = self.query_all_views

        run_query = self.run_query

        user_id: str | Unset = UNSET
        if not isinstance(self.user_id, Unset):
            user_id = str(self.user_id)

        workbook_url = self.workbook_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "modelId": model_id,
                "prompt": prompt,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if current_topic_name is not UNSET:
            field_dict["currentTopicName"] = current_topic_name
        if query_all_views is not UNSET:
            field_dict["queryAllViews"] = query_all_views
        if run_query is not UNSET:
            field_dict["runQuery"] = run_query
        if user_id is not UNSET:
            field_dict["userId"] = user_id
        if workbook_url is not UNSET:
            field_dict["workbookUrl"] = workbook_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model_id = UUID(d.pop("modelId"))

        prompt = d.pop("prompt")

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        current_topic_name = d.pop("currentTopicName", UNSET)

        query_all_views = d.pop("queryAllViews", UNSET)

        run_query = d.pop("runQuery", UNSET)

        _user_id = d.pop("userId", UNSET)
        user_id: UUID | Unset
        if isinstance(_user_id, Unset):
            user_id = UNSET
        else:
            user_id = UUID(_user_id)

        workbook_url = d.pop("workbookUrl", UNSET)

        ai_generate_query_body = cls(
            model_id=model_id,
            prompt=prompt,
            branch_id=branch_id,
            current_topic_name=current_topic_name,
            query_all_views=query_all_views,
            run_query=run_query,
            user_id=user_id,
            workbook_url=workbook_url,
        )

        ai_generate_query_body.additional_properties = d
        return ai_generate_query_body

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
