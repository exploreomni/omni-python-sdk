from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EvalPrompt")


@_attrs_define
class EvalPrompt:
    """
    Attributes:
        created_at (None | str): ISO 8601 timestamp when the prompt was created. Example: 2025-01-15T10:00:00.000Z.
        expectation (None | str): The expectation the analysis judge scores the analysis against, or null when none was
            set. Example: The top product by revenue should be Aniseed Syrup..
        id (UUID): Unique identifier for the prompt. Example: 770e8400-e29b-41d4-a716-446655440002.
        prompt_text (str): The natural language prompt text the AI is evaluated on. Example: What are the top 5 products
            by revenue?.
        updated_at (None | str): ISO 8601 timestamp when the prompt was last updated. Example: 2025-01-15T10:00:00.000Z.
    """

    created_at: None | str
    expectation: None | str
    id: UUID
    prompt_text: str
    updated_at: None | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at: None | str
        created_at = self.created_at

        expectation: None | str
        expectation = self.expectation

        id = str(self.id)

        prompt_text = self.prompt_text

        updated_at: None | str
        updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "created_at": created_at,
                "expectation": expectation,
                "id": id,
                "prompt_text": prompt_text,
                "updated_at": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_created_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        created_at = _parse_created_at(d.pop("created_at"))

        def _parse_expectation(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        expectation = _parse_expectation(d.pop("expectation"))

        id = UUID(d.pop("id"))

        prompt_text = d.pop("prompt_text")

        def _parse_updated_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        updated_at = _parse_updated_at(d.pop("updated_at"))

        eval_prompt = cls(
            created_at=created_at,
            expectation=expectation,
            id=id,
            prompt_text=prompt_text,
            updated_at=updated_at,
        )

        eval_prompt.additional_properties = d
        return eval_prompt

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
