from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_prompt_sets_update_body_prompts_item import EvalPromptSetsUpdateBodyPromptsItem


T = TypeVar("T", bound="EvalPromptSetsUpdateBody")


@_attrs_define
class EvalPromptSetsUpdateBody:
    """
    Attributes:
        description (None | str | Unset): New description for the prompt set. Pass `null` to clear. Max 1024 characters.
        name (str | Unset): New human-readable name for the prompt set. 255 characters or fewer.
        prompts (list[EvalPromptSetsUpdateBodyPromptsItem] | Unset): Full desired set of prompts after the update.
            Prompts omitted from this list are deleted; new prompts (no `id`) are appended in body order. Existing prompts
            retain their original position — reordering is not supported on this endpoint. At most 25 prompts total.
    """

    description: None | str | Unset = UNSET
    name: str | Unset = UNSET
    prompts: list[EvalPromptSetsUpdateBodyPromptsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        name = self.name

        prompts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.prompts, Unset):
            prompts = []
            for prompts_item_data in self.prompts:
                prompts_item = prompts_item_data.to_dict()
                prompts.append(prompts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if name is not UNSET:
            field_dict["name"] = name
        if prompts is not UNSET:
            field_dict["prompts"] = prompts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_prompt_sets_update_body_prompts_item import EvalPromptSetsUpdateBodyPromptsItem

        d = dict(src_dict)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        name = d.pop("name", UNSET)

        _prompts = d.pop("prompts", UNSET)
        prompts: list[EvalPromptSetsUpdateBodyPromptsItem] | Unset = UNSET
        if _prompts is not UNSET:
            prompts = []
            for prompts_item_data in _prompts:
                prompts_item = EvalPromptSetsUpdateBodyPromptsItem.from_dict(prompts_item_data)

                prompts.append(prompts_item)

        eval_prompt_sets_update_body = cls(
            description=description,
            name=name,
            prompts=prompts,
        )

        eval_prompt_sets_update_body.additional_properties = d
        return eval_prompt_sets_update_body

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
