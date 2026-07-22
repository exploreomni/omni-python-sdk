from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.documents_add_permits_body_role import DocumentsAddPermitsBodyRole, check_documents_add_permits_body_role
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsAddPermitsBody")


@_attrs_define
class DocumentsAddPermitsBody:
    """
    Attributes:
        role (DocumentsAddPermitsBodyRole): Role to grant
        access_boost (bool | Unset): Grant access boost Default: False.
        user_group_ids (list[str] | Unset): User group IDs to grant access to
        user_ids (list[UUID] | Unset): User membership IDs to grant access to
    """

    role: DocumentsAddPermitsBodyRole
    access_boost: bool | Unset = False
    user_group_ids: list[str] | Unset = UNSET
    user_ids: list[UUID] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role: str = self.role

        access_boost = self.access_boost

        user_group_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_group_ids, Unset):
            user_group_ids = self.user_group_ids

        user_ids: list[str] | Unset = UNSET
        if not isinstance(self.user_ids, Unset):
            user_ids = []
            for user_ids_item_data in self.user_ids:
                user_ids_item = str(user_ids_item_data)
                user_ids.append(user_ids_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "role": role,
            }
        )
        if access_boost is not UNSET:
            field_dict["accessBoost"] = access_boost
        if user_group_ids is not UNSET:
            field_dict["userGroupIds"] = user_group_ids
        if user_ids is not UNSET:
            field_dict["userIds"] = user_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        role = check_documents_add_permits_body_role(d.pop("role"))

        access_boost = d.pop("accessBoost", UNSET)

        user_group_ids = cast(list[str], d.pop("userGroupIds", UNSET))

        _user_ids = d.pop("userIds", UNSET)
        user_ids: list[UUID] | Unset = UNSET
        if _user_ids is not UNSET:
            user_ids = []
            for user_ids_item_data in _user_ids:
                user_ids_item = UUID(user_ids_item_data)

                user_ids.append(user_ids_item)

        documents_add_permits_body = cls(
            role=role,
            access_boost=access_boost,
            user_group_ids=user_group_ids,
            user_ids=user_ids,
        )

        documents_add_permits_body.additional_properties = d
        return documents_add_permits_body

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
