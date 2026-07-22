from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ModelsCacheResetResponseCacheReset")


@_attrs_define
class ModelsCacheResetResponseCacheReset:
    """Cache reset details

    Attributes:
        created_at (None | str): Creation timestamp
        model_id (str): Model ID
        policy_name (str): Cache policy name
        reset_at (None | str): Reset timestamp
        updated_at (None | str): Last update timestamp
    """

    created_at: None | str
    model_id: str
    policy_name: str
    reset_at: None | str
    updated_at: None | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at: None | str
        created_at = self.created_at

        model_id = self.model_id

        policy_name = self.policy_name

        reset_at: None | str
        reset_at = self.reset_at

        updated_at: None | str
        updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "created_at": created_at,
                "model_id": model_id,
                "policy_name": policy_name,
                "reset_at": reset_at,
                "updated_at": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_created_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        created_at = _parse_created_at(d.pop("created_at"))

        model_id = d.pop("model_id")

        policy_name = d.pop("policy_name")

        def _parse_reset_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        reset_at = _parse_reset_at(d.pop("reset_at"))

        def _parse_updated_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        updated_at = _parse_updated_at(d.pop("updated_at"))

        models_cache_reset_response_cache_reset = cls(
            created_at=created_at,
            model_id=model_id,
            policy_name=policy_name,
            reset_at=reset_at,
            updated_at=updated_at,
        )

        models_cache_reset_response_cache_reset.additional_properties = d
        return models_cache_reset_response_cache_reset

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
