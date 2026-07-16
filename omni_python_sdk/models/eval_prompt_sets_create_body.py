from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_prompt_sets_create_body_prompts_item import EvalPromptSetsCreateBodyPromptsItem


T = TypeVar("T", bound="EvalPromptSetsCreateBody")


@_attrs_define
class EvalPromptSetsCreateBody:
    """
    Attributes:
        model_id (UUID): The shared model this prompt set is bound to. Example: 880e8400-e29b-41d4-a716-446655440003.
        name (str): Human-readable name for the prompt set. 255 characters or fewer. Example: Orders regression.
        slug (str): URL-safe identifier for the prompt set. Must be unique per `model_id` and match `^[a-z][a-z0-9-]*$`.
            Max 255 characters. Example: orders-regression.
        description (None | str | Unset): Optional human-readable description of the prompt set. Max 1024 characters.
            Example: Regression suite for the orders topic.
        prompts (list[EvalPromptSetsCreateBodyPromptsItem] | Unset): Initial prompts for the set. Defaults to an empty
            list. At most 25 prompts.
    """

    model_id: UUID
    name: str
    slug: str
    description: None | str | Unset = UNSET
    prompts: list[EvalPromptSetsCreateBodyPromptsItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = str(self.model_id)

        name = self.name

        slug = self.slug

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        prompts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.prompts, Unset):
            prompts = []
            for prompts_item_data in self.prompts:
                prompts_item = prompts_item_data.to_dict()
                prompts.append(prompts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model_id": model_id,
                "name": name,
                "slug": slug,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if prompts is not UNSET:
            field_dict["prompts"] = prompts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_prompt_sets_create_body_prompts_item import EvalPromptSetsCreateBodyPromptsItem

        d = dict(src_dict)
        model_id = UUID(d.pop("model_id"))

        name = d.pop("name")

        slug = d.pop("slug")

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        _prompts = d.pop("prompts", UNSET)
        prompts: list[EvalPromptSetsCreateBodyPromptsItem] | Unset = UNSET
        if _prompts is not UNSET:
            prompts = []
            for prompts_item_data in _prompts:
                prompts_item = EvalPromptSetsCreateBodyPromptsItem.from_dict(prompts_item_data)

                prompts.append(prompts_item)

        eval_prompt_sets_create_body = cls(
            model_id=model_id,
            name=name,
            slug=slug,
            description=description,
            prompts=prompts,
        )

        eval_prompt_sets_create_body.additional_properties = d
        return eval_prompt_sets_create_body

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
