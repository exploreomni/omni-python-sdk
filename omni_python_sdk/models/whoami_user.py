from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="WhoamiUser")


@_attrs_define
class WhoamiUser:
    """
    Attributes:
        id (str): The caller's user id
        membership_id (str): The caller's own membership id within this organization. This is the id accepted by the
            admin `GET /api/v1/users/{id}/model-roles` endpoint (it is distinct from the user id).
    """

    id: str
    membership_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        membership_id = self.membership_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "membershipId": membership_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        membership_id = d.pop("membershipId")

        whoami_user = cls(
            id=id,
            membership_id=membership_id,
        )

        whoami_user.additional_properties = d
        return whoami_user

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
