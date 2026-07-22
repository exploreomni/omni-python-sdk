from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.user_group_role_origin import UserGroupRoleOrigin


T = TypeVar("T", bound="UserGroupRoleAssignmentResult")


@_attrs_define
class UserGroupRoleAssignmentResult:
    """
    Attributes:
        base_role (str): The base role definition name Example: VIEWER.
        connection_id (UUID): Connection this role applies to
        from_ (UserGroupRoleOrigin): Origin of this role assignment
        model_id (UUID): Model this role applies to
        priority (float): Priority for role resolution (higher = more permissive)
        role_name (str): The role name (base or custom) Example: VIEWER.
    """

    base_role: str
    connection_id: UUID
    from_: UserGroupRoleOrigin
    model_id: UUID
    priority: float
    role_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_role = self.base_role

        connection_id = str(self.connection_id)

        from_ = self.from_.to_dict()

        model_id = str(self.model_id)

        priority = self.priority

        role_name = self.role_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseRole": base_role,
                "connectionId": connection_id,
                "from": from_,
                "modelId": model_id,
                "priority": priority,
                "roleName": role_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.user_group_role_origin import UserGroupRoleOrigin

        d = dict(src_dict)
        base_role = d.pop("baseRole")

        connection_id = UUID(d.pop("connectionId"))

        from_ = UserGroupRoleOrigin.from_dict(d.pop("from"))

        model_id = UUID(d.pop("modelId"))

        priority = d.pop("priority")

        role_name = d.pop("roleName")

        user_group_role_assignment_result = cls(
            base_role=base_role,
            connection_id=connection_id,
            from_=from_,
            model_id=model_id,
            priority=priority,
            role_name=role_name,
        )

        user_group_role_assignment_result.additional_properties = d
        return user_group_role_assignment_result

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
