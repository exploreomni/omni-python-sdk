from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_run_detail import EvalRunDetail


T = TypeVar("T", bound="EvalRunsCancelResponse")


@_attrs_define
class EvalRunsCancelResponse:
    """
    Attributes:
        cancelled (int): Number of per-prompt agentic jobs that were cancelled by this request. Example: 4.
        run (EvalRunDetail): The newly created run with its initial results.
        total (int): Total number of per-prompt jobs in the run. Example: 12.
    """

    cancelled: int
    run: EvalRunDetail
    total: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cancelled = self.cancelled

        run = self.run.to_dict()

        total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "cancelled": cancelled,
                "run": run,
                "total": total,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_run_detail import EvalRunDetail

        d = dict(src_dict)
        cancelled = d.pop("cancelled")

        run = EvalRunDetail.from_dict(d.pop("run"))

        total = d.pop("total")

        eval_runs_cancel_response = cls(
            cancelled=cancelled,
            run=run,
            total=total,
        )

        eval_runs_cancel_response.additional_properties = d
        return eval_runs_cancel_response

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
