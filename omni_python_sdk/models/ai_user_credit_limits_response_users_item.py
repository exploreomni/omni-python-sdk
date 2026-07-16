from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiUserCreditLimitsResponseUsersItem")


@_attrs_define
class AiUserCreditLimitsResponseUsersItem:
    """
    Attributes:
        credit_limit (float | None): The user's effective AI credit limit, or `null` for unlimited. Example: 50.
        user_id (str): The user's id within this organization. Example: f4a2b3c8-0d1e-4f5a-9b6c-7d8e9f0a1b2c.
        uses_default_limit (bool): True when the user has no individual limit and follows the org default.
    """

    credit_limit: float | None
    user_id: str
    uses_default_limit: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        credit_limit: float | None
        credit_limit = self.credit_limit

        user_id = self.user_id

        uses_default_limit = self.uses_default_limit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "creditLimit": credit_limit,
                "userId": user_id,
                "usesDefaultLimit": uses_default_limit,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_credit_limit(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        credit_limit = _parse_credit_limit(d.pop("creditLimit"))

        user_id = d.pop("userId")

        uses_default_limit = d.pop("usesDefaultLimit")

        ai_user_credit_limits_response_users_item = cls(
            credit_limit=credit_limit,
            user_id=user_id,
            uses_default_limit=uses_default_limit,
        )

        ai_user_credit_limits_response_users_item.additional_properties = d
        return ai_user_credit_limits_response_users_item

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
