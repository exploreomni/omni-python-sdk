from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiTopicParams")


@_attrs_define
class AiTopicParams:
    """
    Attributes:
        model_id (UUID): The UUID of the shared model to query against. Only shared models are supported. Example:
            770e8400-e29b-41d4-a716-446655440002.
        branch_id (UUID | Unset): Optional branch ID for the model. Must be a branch of the shared model specified by
            modelId. Example: 550e8400-e29b-41d4-a716-446655440000.
        current_topic_name (str | Unset): The name of the current topic to scope query generation. If not provided, AI
            will automatically select the best topic for your prompt. Example: order_items.
    """

    model_id: UUID
    branch_id: UUID | Unset = UNSET
    current_topic_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = str(self.model_id)

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        current_topic_name = self.current_topic_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "modelId": model_id,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if current_topic_name is not UNSET:
            field_dict["currentTopicName"] = current_topic_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model_id = UUID(d.pop("modelId"))

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        current_topic_name = d.pop("currentTopicName", UNSET)

        ai_topic_params = cls(
            model_id=model_id,
            branch_id=branch_id,
            current_topic_name=current_topic_name,
        )

        ai_topic_params.additional_properties = d
        return ai_topic_params

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
