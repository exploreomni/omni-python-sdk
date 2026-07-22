from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiPickTopicBody")


@_attrs_define
class AiPickTopicBody:
    """
    Attributes:
        model_id (UUID): The UUID of the shared model to query against. Only shared models are supported. Example:
            770e8400-e29b-41d4-a716-446655440002.
        prompt (str): The natural language prompt to analyze. The AI will determine which topic best matches the data
            described in this prompt. Example: How many orders were placed last month?.
        branch_id (UUID | Unset): Optional branch ID for the model. Must be a branch of the shared model specified by
            modelId. Example: 550e8400-e29b-41d4-a716-446655440000.
        current_topic_name (str | Unset): The name of the current topic to scope query generation. If not provided, AI
            will automatically select the best topic for your prompt. Example: order_items.
        potential_topic_names (list[str] | Unset): Optional list of topic names to limit consideration to. If not
            provided, all topics the user has access to in the model will be evaluated. Example: ['order_items',
            'customers', 'products'].
        user_id (UUID | Unset): User ID to evaluate topic access as. Their permissions will be used for permission-aware
            topic selection. Only valid with organization-scoped API keys. Personal access tokens always act as the
            authenticated user. Example: 990e8400-e29b-41d4-a716-446655440004.
    """

    model_id: UUID
    prompt: str
    branch_id: UUID | Unset = UNSET
    current_topic_name: str | Unset = UNSET
    potential_topic_names: list[str] | Unset = UNSET
    user_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = str(self.model_id)

        prompt = self.prompt

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        current_topic_name = self.current_topic_name

        potential_topic_names: list[str] | Unset = UNSET
        if not isinstance(self.potential_topic_names, Unset):
            potential_topic_names = self.potential_topic_names

        user_id: str | Unset = UNSET
        if not isinstance(self.user_id, Unset):
            user_id = str(self.user_id)

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
        if potential_topic_names is not UNSET:
            field_dict["potentialTopicNames"] = potential_topic_names
        if user_id is not UNSET:
            field_dict["userId"] = user_id

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

        potential_topic_names = cast(list[str], d.pop("potentialTopicNames", UNSET))

        _user_id = d.pop("userId", UNSET)
        user_id: UUID | Unset
        if isinstance(_user_id, Unset):
            user_id = UNSET
        else:
            user_id = UUID(_user_id)

        ai_pick_topic_body = cls(
            model_id=model_id,
            prompt=prompt,
            branch_id=branch_id,
            current_topic_name=current_topic_name,
            potential_topic_names=potential_topic_names,
            user_id=user_id,
        )

        ai_pick_topic_body.additional_properties = d
        return ai_pick_topic_body

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
