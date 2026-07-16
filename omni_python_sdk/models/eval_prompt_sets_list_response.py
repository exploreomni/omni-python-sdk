from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_prompt_set_list_item import EvalPromptSetListItem


T = TypeVar("T", bound="EvalPromptSetsListResponse")


@_attrs_define
class EvalPromptSetsListResponse:
    """
    Attributes:
        prompt_sets (list[EvalPromptSetListItem]): Prompt sets matching the query, sorted alphabetically by name.
    """

    prompt_sets: list[EvalPromptSetListItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prompt_sets = []
        for prompt_sets_item_data in self.prompt_sets:
            prompt_sets_item = prompt_sets_item_data.to_dict()
            prompt_sets.append(prompt_sets_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "prompt_sets": prompt_sets,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_prompt_set_list_item import EvalPromptSetListItem

        d = dict(src_dict)
        prompt_sets = []
        _prompt_sets = d.pop("prompt_sets")
        for prompt_sets_item_data in _prompt_sets:
            prompt_sets_item = EvalPromptSetListItem.from_dict(prompt_sets_item_data)

            prompt_sets.append(prompt_sets_item)

        eval_prompt_sets_list_response = cls(
            prompt_sets=prompt_sets,
        )

        eval_prompt_sets_list_response.additional_properties = d
        return eval_prompt_sets_list_response

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
