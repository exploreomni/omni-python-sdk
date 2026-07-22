from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.role_origin_type_0 import RoleOriginType0
    from ..models.role_origin_type_1 import RoleOriginType1
    from ..models.role_origin_type_2 import RoleOriginType2
    from ..models.role_origin_type_3 import RoleOriginType3


T = TypeVar("T", bound="RoleAssignmentResult")


@_attrs_define
class RoleAssignmentResult:
    """
    Attributes:
        base_role (str): The base role definition name Example: VIEWER.
        connection_id (UUID): Connection this role applies to
        from_ (RoleOriginType0 | RoleOriginType1 | RoleOriginType2 | RoleOriginType3): Origin of this role assignment
        model_id (UUID): Model this role applies to
        priority (float): Priority for role resolution (higher = more permissive)
        resolved (bool): Whether this is the resolved (effective) role
        role_name (str): The role name (base or custom) Example: VIEWER.
    """

    base_role: str
    connection_id: UUID
    from_: RoleOriginType0 | RoleOriginType1 | RoleOriginType2 | RoleOriginType3
    model_id: UUID
    priority: float
    resolved: bool
    role_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.role_origin_type_0 import RoleOriginType0
        from ..models.role_origin_type_1 import RoleOriginType1
        from ..models.role_origin_type_2 import RoleOriginType2

        base_role = self.base_role

        connection_id = str(self.connection_id)

        from_: dict[str, Any]
        if isinstance(self.from_, RoleOriginType0):
            from_ = self.from_.to_dict()
        elif isinstance(self.from_, RoleOriginType1):
            from_ = self.from_.to_dict()
        elif isinstance(self.from_, RoleOriginType2):
            from_ = self.from_.to_dict()
        else:
            from_ = self.from_.to_dict()

        model_id = str(self.model_id)

        priority = self.priority

        resolved = self.resolved

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
                "resolved": resolved,
                "roleName": role_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.role_origin_type_0 import RoleOriginType0
        from ..models.role_origin_type_1 import RoleOriginType1
        from ..models.role_origin_type_2 import RoleOriginType2
        from ..models.role_origin_type_3 import RoleOriginType3

        d = dict(src_dict)
        base_role = d.pop("baseRole")

        connection_id = UUID(d.pop("connectionId"))

        def _parse_from_(data: object) -> RoleOriginType0 | RoleOriginType1 | RoleOriginType2 | RoleOriginType3:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_role_origin_type_0 = RoleOriginType0.from_dict(data)

                return componentsschemas_role_origin_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_role_origin_type_1 = RoleOriginType1.from_dict(data)

                return componentsschemas_role_origin_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_role_origin_type_2 = RoleOriginType2.from_dict(data)

                return componentsschemas_role_origin_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_role_origin_type_3 = RoleOriginType3.from_dict(data)

            return componentsschemas_role_origin_type_3

        from_ = _parse_from_(d.pop("from"))

        model_id = UUID(d.pop("modelId"))

        priority = d.pop("priority")

        resolved = d.pop("resolved")

        role_name = d.pop("roleName")

        role_assignment_result = cls(
            base_role=base_role,
            connection_id=connection_id,
            from_=from_,
            model_id=model_id,
            priority=priority,
            resolved=resolved,
            role_name=role_name,
        )

        role_assignment_result.additional_properties = d
        return role_assignment_result

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
