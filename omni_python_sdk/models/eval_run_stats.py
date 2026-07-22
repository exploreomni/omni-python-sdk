from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EvalRunStats")


@_attrs_define
class EvalRunStats:
    """
    Attributes:
        terminal (int): Number of per-prompt jobs that have reached a terminal state (COMPLETE, FAILED, or CANCELLED).
            Example: 8.
        total (int): Total number of jobs in the run (prompts × the repeat count). Example: 12.
    """

    terminal: int
    total: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        terminal = self.terminal

        total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "terminal": terminal,
                "total": total,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        terminal = d.pop("terminal")

        total = d.pop("total")

        eval_run_stats = cls(
            terminal=terminal,
            total=total,
        )

        eval_run_stats.additional_properties = d
        return eval_run_stats

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
