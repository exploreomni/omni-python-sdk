from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.scim_groups_replace_body_members_item import ScimGroupsReplaceBodyMembersItem


T = TypeVar("T", bound="ScimGroupsReplaceBody")


@_attrs_define
class ScimGroupsReplaceBody:
    """
    Attributes:
        display_name (str): Display name of the group Example: Engineering Team.
        members (list[ScimGroupsReplaceBodyMembersItem]): List of group members
    """

    display_name: str
    members: list[ScimGroupsReplaceBodyMembersItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        display_name = self.display_name

        members = []
        for members_item_data in self.members:
            members_item = members_item_data.to_dict()
            members.append(members_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "displayName": display_name,
                "members": members,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_groups_replace_body_members_item import ScimGroupsReplaceBodyMembersItem

        d = dict(src_dict)
        display_name = d.pop("displayName")

        members = []
        _members = d.pop("members")
        for members_item_data in _members:
            members_item = ScimGroupsReplaceBodyMembersItem.from_dict(members_item_data)

            members.append(members_item)

        scim_groups_replace_body = cls(
            display_name=display_name,
            members=members,
        )

        scim_groups_replace_body.additional_properties = d
        return scim_groups_replace_body

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
