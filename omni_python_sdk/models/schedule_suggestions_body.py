from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ScheduleSuggestionsBody")


@_attrs_define
class ScheduleSuggestionsBody:
    """
    Attributes:
        timezone (str | Unset): IANA timezone the schedule fires in (e.g. `America/New_York`). Generation currently runs
            once daily at ~2 AM in this timezone. Defaults to `UTC`. Default: 'UTC'. Example: America/New_York.
    """

    timezone: str | Unset = "UTC"

    def to_dict(self) -> dict[str, Any]:
        timezone = self.timezone

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if timezone is not UNSET:
            field_dict["timezone"] = timezone

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        timezone = d.pop("timezone", UNSET)

        schedule_suggestions_body = cls(
            timezone=timezone,
        )

        return schedule_suggestions_body
