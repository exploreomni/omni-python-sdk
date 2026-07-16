from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.ai_user_credit_limits_response_users_item import AiUserCreditLimitsResponseUsersItem


T = TypeVar("T", bound="AiUserCreditLimitsResponse")


@_attrs_define
class AiUserCreditLimitsResponse:
    """
    Attributes:
        users (list[AiUserCreditLimitsResponseUsersItem]):
    """

    users: list[AiUserCreditLimitsResponseUsersItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        users = []
        for users_item_data in self.users:
            users_item = users_item_data.to_dict()
            users.append(users_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "users": users,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_user_credit_limits_response_users_item import AiUserCreditLimitsResponseUsersItem

        d = dict(src_dict)
        users = []
        _users = d.pop("users")
        for users_item_data in _users:
            users_item = AiUserCreditLimitsResponseUsersItem.from_dict(users_item_data)

            users.append(users_item)

        ai_user_credit_limits_response = cls(
            users=users,
        )

        ai_user_credit_limits_response.additional_properties = d
        return ai_user_credit_limits_response

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
