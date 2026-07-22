from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_runs_create_body_run_config import EvalRunsCreateBodyRunConfig


T = TypeVar("T", bound="EvalRunsCreateBody")


@_attrs_define
class EvalRunsCreateBody:
    """
    Attributes:
        prompt_set_id (UUID): The prompt set to execute. Example: 550e8400-e29b-41d4-a716-446655440000.
        description (None | str | Unset): Optional human-readable description for the run. Pass `null` to clear (or
            omit). Max 1024 characters. Example: Re-running after switching to gpt-4o for query generation.
        run_config (EvalRunsCreateBodyRunConfig | Unset): Per-run configuration. Optional — omit if no overrides.
    """

    prompt_set_id: UUID
    description: None | str | Unset = UNSET
    run_config: EvalRunsCreateBodyRunConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prompt_set_id = str(self.prompt_set_id)

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        run_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.run_config, Unset):
            run_config = self.run_config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "prompt_set_id": prompt_set_id,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if run_config is not UNSET:
            field_dict["run_config"] = run_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_runs_create_body_run_config import EvalRunsCreateBodyRunConfig

        d = dict(src_dict)
        prompt_set_id = UUID(d.pop("prompt_set_id"))

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        _run_config = d.pop("run_config", UNSET)
        run_config: EvalRunsCreateBodyRunConfig | Unset
        if isinstance(_run_config, Unset):
            run_config = UNSET
        else:
            run_config = EvalRunsCreateBodyRunConfig.from_dict(_run_config)

        eval_runs_create_body = cls(
            prompt_set_id=prompt_set_id,
            description=description,
            run_config=run_config,
        )

        eval_runs_create_body.additional_properties = d
        return eval_runs_create_body

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
