from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_agent_action_kind import AiAgentActionKind, check_ai_agent_action_kind

T = TypeVar("T", bound="AiAgentAction")


@_attrs_define
class AiAgentAction:
    """
    Attributes:
        kind (AiAgentActionKind): Source of the entry: `sample` for `sample_queries` (model- or topic-level) and `skill`
            for `skills` (model- or topic-level). Example: skill.
        label (str): Short, human-readable name for the action — chip text in client UIs and the visible "prompt" on the
            answer card. Example: Revenue trends.
        prompt (str): Submit this string verbatim as the `prompt` on `POST /api/v1/ai/jobs`. For sample queries this is
            the raw prompt; for skills it is a pre-formatted wrapper around the skill's input. Example: Skill:
            Show me the recent revenue trends grouped by month….
    """

    kind: AiAgentActionKind
    label: str
    prompt: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind: str = self.kind

        label = self.label

        prompt = self.prompt

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "label": label,
                "prompt": prompt,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = check_ai_agent_action_kind(d.pop("kind"))

        label = d.pop("label")

        prompt = d.pop("prompt")

        ai_agent_action = cls(
            kind=kind,
            label=label,
            prompt=prompt,
        )

        ai_agent_action.additional_properties = d
        return ai_agent_action

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
