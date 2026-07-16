from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiCreditControlsUsersListResponseRecordsItem")


@_attrs_define
class AiCreditControlsUsersListResponseRecordsItem:
    """
    Attributes:
        credit_limit (float | None): The user's individual AI credit limit, or `null` for an explicit unlimited
            override. Example: 50.
        user_id (str): The user's id within this organization. Example: f4a2b3c8-0d1e-4f5a-9b6c-7d8e9f0a1b2c.
    """

    credit_limit: float | None
    user_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        credit_limit: float | None
        credit_limit = self.credit_limit

        user_id = self.user_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "creditLimit": credit_limit,
                "userId": user_id,
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

        ai_credit_controls_users_list_response_records_item = cls(
            credit_limit=credit_limit,
            user_id=user_id,
        )

        ai_credit_controls_users_list_response_records_item.additional_properties = d
        return ai_credit_controls_users_list_response_records_item

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
