from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EvalRunsCreateBodyRunConfig")


@_attrs_define
class EvalRunsCreateBodyRunConfig:
    """Per-run configuration. Optional — omit if no overrides.

    Attributes:
        branch_id (UUID | Unset): Optional branch ID to run against. Must be a branch of the prompt set's model.
            Example: 440e8400-e29b-41d4-a716-446655440006.
    """

    branch_id: UUID | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if branch_id is not UNSET:
            field_dict["branch_id"] = branch_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _branch_id = d.pop("branch_id", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        eval_runs_create_body_run_config = cls(
            branch_id=branch_id,
        )

        eval_runs_create_body_run_config.additional_properties = d
        return eval_runs_create_body_run_config

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
