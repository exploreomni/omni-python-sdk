from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ai_job_action import AiJobAction


T = TypeVar("T", bound="AiJobResultResponse")


@_attrs_define
class AiJobResultResponse:
    """
    Attributes:
        actions (list[AiJobAction] | Unset): Ordered list of actions the AI took during execution. Each action
            represents a step such as generating a query, executing it, or synthesizing a final answer.
        message (str | Unset): The AI's final response message in Markdown format. This is the complete answer to the
            original prompt, incorporating data from all executed queries. Example: ### Top 5 Products by Revenue

            1. **Sunglasses** - $678,994
            2. **Jeans** - $475,072.
        omni_chat_url (str | Unset): URL to view this conversation in the Omni chat interface. Opens the chat session
            where the job actions and results are visible. Example: https://my-
            org.omni.co/chat/660e8400-e29b-41d4-a716-446655440001.
        result_summary (str | Unset): Summary of the job result. Typically matches the final message content. Example:
            ### Top 5 Products by Revenue

            1. **Sunglasses** - $678,994
            2. **Jeans** - $475,072.
        topic (str | Unset): The topic name used for query generation. Example: order_items.
    """

    actions: list[AiJobAction] | Unset = UNSET
    message: str | Unset = UNSET
    omni_chat_url: str | Unset = UNSET
    result_summary: str | Unset = UNSET
    topic: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        actions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.actions, Unset):
            actions = []
            for actions_item_data in self.actions:
                actions_item = actions_item_data.to_dict()
                actions.append(actions_item)

        message = self.message

        omni_chat_url = self.omni_chat_url

        result_summary = self.result_summary

        topic = self.topic

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if actions is not UNSET:
            field_dict["actions"] = actions
        if message is not UNSET:
            field_dict["message"] = message
        if omni_chat_url is not UNSET:
            field_dict["omniChatUrl"] = omni_chat_url
        if result_summary is not UNSET:
            field_dict["resultSummary"] = result_summary
        if topic is not UNSET:
            field_dict["topic"] = topic

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_job_action import AiJobAction

        d = dict(src_dict)
        _actions = d.pop("actions", UNSET)
        actions: list[AiJobAction] | Unset = UNSET
        if _actions is not UNSET:
            actions = []
            for actions_item_data in _actions:
                actions_item = AiJobAction.from_dict(actions_item_data)

                actions.append(actions_item)

        message = d.pop("message", UNSET)

        omni_chat_url = d.pop("omniChatUrl", UNSET)

        result_summary = d.pop("resultSummary", UNSET)

        topic = d.pop("topic", UNSET)

        ai_job_result_response = cls(
            actions=actions,
            message=message,
            omni_chat_url=omni_chat_url,
            result_summary=result_summary,
            topic=topic,
        )

        ai_job_result_response.additional_properties = d
        return ai_job_result_response

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
