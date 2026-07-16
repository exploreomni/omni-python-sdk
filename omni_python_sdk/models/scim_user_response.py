from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ScimUserResponse")


@_attrs_define
class ScimUserResponse:
    """
    Attributes:
        active (bool): Whether the user is active
        display_name (str): Display name
        id (UUID): SCIM user ID
        schemas (list[str]): SCIM schema URIs
        user_name (str): Username (email)
    """

    active: bool
    display_name: str
    id: UUID
    schemas: list[str]
    user_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        active = self.active

        display_name = self.display_name

        id = str(self.id)

        schemas = self.schemas

        user_name = self.user_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "active": active,
                "displayName": display_name,
                "id": id,
                "schemas": schemas,
                "userName": user_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        active = d.pop("active")

        display_name = d.pop("displayName")

        id = UUID(d.pop("id"))

        schemas = cast(list[str], d.pop("schemas"))

        user_name = d.pop("userName")

        scim_user_response = cls(
            active=active,
            display_name=display_name,
            id=id,
            schemas=schemas,
            user_name=user_name,
        )

        scim_user_response.additional_properties = d
        return scim_user_response

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
