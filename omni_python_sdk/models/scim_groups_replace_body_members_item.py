from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ScimGroupsReplaceBodyMembersItem")


@_attrs_define
class ScimGroupsReplaceBodyMembersItem:
    """
    Attributes:
        display (str): Display name of the member Example: john.doe@example.com.
        value (UUID): User membership ID Example: 550e8400-e29b-41d4-a716-446655440000.
    """

    display: str
    value: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        display = self.display

        value = str(self.value)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "display": display,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        display = d.pop("display")

        value = UUID(d.pop("value"))

        scim_groups_replace_body_members_item = cls(
            display=display,
            value=value,
        )

        scim_groups_replace_body_members_item.additional_properties = d
        return scim_groups_replace_body_members_item

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
