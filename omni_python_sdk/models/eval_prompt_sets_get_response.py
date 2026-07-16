from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_prompt_set import EvalPromptSet


T = TypeVar("T", bound="EvalPromptSetsGetResponse")


@_attrs_define
class EvalPromptSetsGetResponse:
    """
    Attributes:
        prompt_set (EvalPromptSet):
    """

    prompt_set: EvalPromptSet
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prompt_set = self.prompt_set.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "prompt_set": prompt_set,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_prompt_set import EvalPromptSet

        d = dict(src_dict)
        prompt_set = EvalPromptSet.from_dict(d.pop("prompt_set"))

        eval_prompt_sets_get_response = cls(
            prompt_set=prompt_set,
        )

        eval_prompt_sets_get_response.additional_properties = d
        return eval_prompt_sets_get_response

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
