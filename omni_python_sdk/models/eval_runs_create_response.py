from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_run_detail import EvalRunDetail


T = TypeVar("T", bound="EvalRunsCreateResponse")


@_attrs_define
class EvalRunsCreateResponse:
    """
    Attributes:
        job_count (int): Number of per-prompt agentic jobs created for this run (one per prompt that fanned out
            successfully). Enqueue onto the work queue happens after creation and is best-effort, so this count reflects
            jobs created, not necessarily those successfully enqueued. Example: 12.
        run (EvalRunDetail): The newly created run with its initial results.
    """

    job_count: int
    run: EvalRunDetail
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_count = self.job_count

        run = self.run.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_count": job_count,
                "run": run,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_run_detail import EvalRunDetail

        d = dict(src_dict)
        job_count = d.pop("job_count")

        run = EvalRunDetail.from_dict(d.pop("run"))

        eval_runs_create_response = cls(
            job_count=job_count,
            run=run,
        )

        eval_runs_create_response.additional_properties = d
        return eval_runs_create_response

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
