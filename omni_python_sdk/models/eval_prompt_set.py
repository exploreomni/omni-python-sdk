from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_prompt import EvalPrompt


T = TypeVar("T", bound="EvalPromptSet")


@_attrs_define
class EvalPromptSet:
    """
    Attributes:
        created_at (None | str): ISO 8601 timestamp when the prompt set was created. Example: 2025-01-15T10:00:00.000Z.
        description (None | str): Optional human-readable description of the prompt set. Example: Regression suite for
            the orders topic.
        id (UUID): Unique identifier for the prompt set. Example: 550e8400-e29b-41d4-a716-446655440000.
        is_archived (bool): Whether the prompt set has been archived.
        model_id (UUID): The shared model this prompt set is bound to. Example: 880e8400-e29b-41d4-a716-446655440003.
        name (str): Human-readable name for the prompt set. Example: Orders regression.
        prompts (list[EvalPrompt]): Prompts that make up the set.
        slug (str): URL-safe identifier for the prompt set. Unique per `model_id`. Example: orders-regression.
        updated_at (None | str): ISO 8601 timestamp when the prompt set was last updated. Example:
            2025-01-15T10:00:00.000Z.
    """

    created_at: None | str
    description: None | str
    id: UUID
    is_archived: bool
    model_id: UUID
    name: str
    prompts: list[EvalPrompt]
    slug: str
    updated_at: None | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at: None | str
        created_at = self.created_at

        description: None | str
        description = self.description

        id = str(self.id)

        is_archived = self.is_archived

        model_id = str(self.model_id)

        name = self.name

        prompts = []
        for prompts_item_data in self.prompts:
            prompts_item = prompts_item_data.to_dict()
            prompts.append(prompts_item)

        slug = self.slug

        updated_at: None | str
        updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "created_at": created_at,
                "description": description,
                "id": id,
                "is_archived": is_archived,
                "model_id": model_id,
                "name": name,
                "prompts": prompts,
                "slug": slug,
                "updated_at": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_prompt import EvalPrompt

        d = dict(src_dict)

        def _parse_created_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        created_at = _parse_created_at(d.pop("created_at"))

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        id = UUID(d.pop("id"))

        is_archived = d.pop("is_archived")

        model_id = UUID(d.pop("model_id"))

        name = d.pop("name")

        prompts = []
        _prompts = d.pop("prompts")
        for prompts_item_data in _prompts:
            prompts_item = EvalPrompt.from_dict(prompts_item_data)

            prompts.append(prompts_item)

        slug = d.pop("slug")

        def _parse_updated_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        updated_at = _parse_updated_at(d.pop("updated_at"))

        eval_prompt_set = cls(
            created_at=created_at,
            description=description,
            id=id,
            is_archived=is_archived,
            model_id=model_id,
            name=name,
            prompts=prompts,
            slug=slug,
            updated_at=updated_at,
        )

        eval_prompt_set.additional_properties = d
        return eval_prompt_set

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
