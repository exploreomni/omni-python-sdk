from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.ai_user_credit_limit_entry import AiUserCreditLimitEntry


T = TypeVar("T", bound="AiUserCreditLimitsUpdateBody")


@_attrs_define
class AiUserCreditLimitsUpdateBody:
    """
    Attributes:
        users (list[AiUserCreditLimitEntry]): Users to update, at most 1000 per request. Each entry has a `userId` plus
            exactly one of `creditLimit` (number or `null`) or `useDefaultLimit: true`.
    """

    users: list[AiUserCreditLimitEntry]

    def to_dict(self) -> dict[str, Any]:
        users = []
        for users_item_data in self.users:
            users_item = users_item_data.to_dict()
            users.append(users_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "users": users,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_user_credit_limit_entry import AiUserCreditLimitEntry

        d = dict(src_dict)
        users = []
        _users = d.pop("users")
        for users_item_data in _users:
            users_item = AiUserCreditLimitEntry.from_dict(users_item_data)

            users.append(users_item)

        ai_user_credit_limits_update_body = cls(
            users=users,
        )

        return ai_user_credit_limits_update_body
