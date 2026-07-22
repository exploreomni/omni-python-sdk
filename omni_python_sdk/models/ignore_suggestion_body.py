from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="IgnoreSuggestionBody")


@_attrs_define
class IgnoreSuggestionBody:
    """
    Attributes:
        reason (str | Unset): Optional free-text reason for dismissing the suggestion. Example: Already covered by an
            existing field description..
    """

    reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reason = d.pop("reason", UNSET)

        ignore_suggestion_body = cls(
            reason=reason,
        )

        return ignore_suggestion_body
