from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EvalPromptSetsUpdateBodyPromptsItem")


@_attrs_define
class EvalPromptSetsUpdateBodyPromptsItem:
    """
    Attributes:
        prompt_text (str): Updated or new prompt text. Max 8000 characters. Example: What are the top 10 products by
            revenue this quarter?.
        expectation (None | str | Unset): Optional expectation the analysis judge scores the analysis against. Pass
            `null` to clear. Max 16000 characters. Example: The top product by revenue should be Aniseed Syrup..
        id (UUID | Unset): Existing prompt id. When provided, updates that prompt; when omitted, a new prompt is
            created. Prompts not included in this list are removed.
    """

    prompt_text: str
    expectation: None | str | Unset = UNSET
    id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prompt_text = self.prompt_text

        expectation: None | str | Unset
        if isinstance(self.expectation, Unset):
            expectation = UNSET
        else:
            expectation = self.expectation

        id: str | Unset = UNSET
        if not isinstance(self.id, Unset):
            id = str(self.id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "prompt_text": prompt_text,
            }
        )
        if expectation is not UNSET:
            field_dict["expectation"] = expectation
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        prompt_text = d.pop("prompt_text")

        def _parse_expectation(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        expectation = _parse_expectation(d.pop("expectation", UNSET))

        _id = d.pop("id", UNSET)
        id: UUID | Unset
        if isinstance(_id, Unset):
            id = UNSET
        else:
            id = UUID(_id)

        eval_prompt_sets_update_body_prompts_item = cls(
            prompt_text=prompt_text,
            expectation=expectation,
            id=id,
        )

        eval_prompt_sets_update_body_prompts_item.additional_properties = d
        return eval_prompt_sets_update_body_prompts_item

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
