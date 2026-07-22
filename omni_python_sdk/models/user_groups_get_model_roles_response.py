from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.user_group_role_assignment_result import UserGroupRoleAssignmentResult


T = TypeVar("T", bound="UserGroupsGetModelRolesResponse")


@_attrs_define
class UserGroupsGetModelRolesResponse:
    """
    Attributes:
        results (list[UserGroupRoleAssignmentResult]): List of role assignments
        user_group_id (str): The user group short identifier Example: abc123.
    """

    results: list[UserGroupRoleAssignmentResult]
    user_group_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        results = []
        for results_item_data in self.results:
            results_item = results_item_data.to_dict()
            results.append(results_item)

        user_group_id = self.user_group_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "results": results,
                "userGroupId": user_group_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.user_group_role_assignment_result import UserGroupRoleAssignmentResult

        d = dict(src_dict)
        results = []
        _results = d.pop("results")
        for results_item_data in _results:
            results_item = UserGroupRoleAssignmentResult.from_dict(results_item_data)

            results.append(results_item)

        user_group_id = d.pop("userGroupId")

        user_groups_get_model_roles_response = cls(
            results=results,
            user_group_id=user_group_id,
        )

        user_groups_get_model_roles_response.additional_properties = d
        return user_groups_get_model_roles_response

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
