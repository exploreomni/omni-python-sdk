from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.models_cache_reset_response_cache_reset import ModelsCacheResetResponseCacheReset


T = TypeVar("T", bound="ModelsCacheResetResponse")


@_attrs_define
class ModelsCacheResetResponse:
    """
    Attributes:
        cache_reset (ModelsCacheResetResponseCacheReset): Cache reset details
        success (bool): Whether the operation succeeded
    """

    cache_reset: ModelsCacheResetResponseCacheReset
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cache_reset = self.cache_reset.to_dict()

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "cache_reset": cache_reset,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_cache_reset_response_cache_reset import ModelsCacheResetResponseCacheReset

        d = dict(src_dict)
        cache_reset = ModelsCacheResetResponseCacheReset.from_dict(d.pop("cache_reset"))

        success = d.pop("success")

        models_cache_reset_response = cls(
            cache_reset=cache_reset,
            success=success,
        )

        models_cache_reset_response.additional_properties = d
        return models_cache_reset_response

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
