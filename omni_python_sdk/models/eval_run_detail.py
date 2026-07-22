from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.eval_run_detail_status import EvalRunDetailStatus, check_eval_run_detail_status

if TYPE_CHECKING:
    from ..models.eval_run_result import EvalRunResult


T = TypeVar("T", bound="EvalRunDetail")


@_attrs_define
class EvalRunDetail:
    """The newly created run with its initial results.

    Attributes:
        branch_id (None | UUID): Optional branch ID the run was executed against. Null when run against the main shared
            model.
        branch_name (None | str): Display name for the branch, if `branch_id` is set.
        completed_at (None | str): ISO 8601 timestamp when the run reached a terminal state.
        created_at (None | str): ISO 8601 timestamp when the run was created. Example: 2025-01-15T10:00:00.000Z.
        description (None | str): Optional human-readable description for the run.
        id (UUID): Unique identifier for the run. Example: 660e8400-e29b-41d4-a716-446655440001.
        is_archived (bool): Whether the run has been archived.
        model_id (UUID): The shared model this run was executed against. Example: 880e8400-e29b-41d4-a716-446655440003.
        prompt_set_id (UUID): The prompt set this run was created from. Example: 550e8400-e29b-41d4-a716-446655440000.
        repeat_count (int): How many times each prompt in the set was executed. Results carry a `repeat_index` when this
            is greater than 1. Example: 1.
        results (list[EvalRunResult]): Per-execution results for this run (one per prompt, times the repeat count). No
            guaranteed order — group executions by `eval_prompt_id` and order by `repeat_index`.
        run_number (int): Sequential, per-prompt-set run number. Example: 3.
        status (EvalRunDetailStatus): Run-level lifecycle. Flips to a terminal state (COMPLETE or CANCELLED) exactly
            once. Example: RUNNING.
    """

    branch_id: None | UUID
    branch_name: None | str
    completed_at: None | str
    created_at: None | str
    description: None | str
    id: UUID
    is_archived: bool
    model_id: UUID
    prompt_set_id: UUID
    repeat_count: int
    results: list[EvalRunResult]
    run_number: int
    status: EvalRunDetailStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id: None | str
        if isinstance(self.branch_id, UUID):
            branch_id = str(self.branch_id)
        else:
            branch_id = self.branch_id

        branch_name: None | str
        branch_name = self.branch_name

        completed_at: None | str
        completed_at = self.completed_at

        created_at: None | str
        created_at = self.created_at

        description: None | str
        description = self.description

        id = str(self.id)

        is_archived = self.is_archived

        model_id = str(self.model_id)

        prompt_set_id = str(self.prompt_set_id)

        repeat_count = self.repeat_count

        results = []
        for results_item_data in self.results:
            results_item = results_item_data.to_dict()
            results.append(results_item)

        run_number = self.run_number

        status: str = self.status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch_id": branch_id,
                "branch_name": branch_name,
                "completed_at": completed_at,
                "created_at": created_at,
                "description": description,
                "id": id,
                "is_archived": is_archived,
                "model_id": model_id,
                "prompt_set_id": prompt_set_id,
                "repeat_count": repeat_count,
                "results": results,
                "run_number": run_number,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_run_result import EvalRunResult

        d = dict(src_dict)

        def _parse_branch_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                branch_id_type_0 = UUID(data)

                return branch_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        branch_id = _parse_branch_id(d.pop("branch_id"))

        def _parse_branch_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        branch_name = _parse_branch_name(d.pop("branch_name"))

        def _parse_completed_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        completed_at = _parse_completed_at(d.pop("completed_at"))

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

        prompt_set_id = UUID(d.pop("prompt_set_id"))

        repeat_count = d.pop("repeat_count")

        results = []
        _results = d.pop("results")
        for results_item_data in _results:
            results_item = EvalRunResult.from_dict(results_item_data)

            results.append(results_item)

        run_number = d.pop("run_number")

        status = check_eval_run_detail_status(d.pop("status"))

        eval_run_detail = cls(
            branch_id=branch_id,
            branch_name=branch_name,
            completed_at=completed_at,
            created_at=created_at,
            description=description,
            id=id,
            is_archived=is_archived,
            model_id=model_id,
            prompt_set_id=prompt_set_id,
            repeat_count=repeat_count,
            results=results,
            run_number=run_number,
            status=status,
        )

        eval_run_detail.additional_properties = d
        return eval_run_detail

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
