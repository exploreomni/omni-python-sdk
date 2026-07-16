from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsCacheResetBody")


@_attrs_define
class ModelsCacheResetBody:
    """
    Attributes:
        reset_at (str | Unset): ISO-8601 timestamp for when to reset the cache Example: 2024-01-15T12:00:00Z.
    """

    reset_at: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reset_at = self.reset_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if reset_at is not UNSET:
            field_dict["resetAt"] = reset_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reset_at = d.pop("resetAt", UNSET)

        models_cache_reset_body = cls(
            reset_at=reset_at,
        )

        models_cache_reset_body.additional_properties = d
        return models_cache_reset_body

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
