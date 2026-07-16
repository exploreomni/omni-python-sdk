from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_groups_patch_body_operations_item_type_1_op import (
    ScimGroupsPatchBodyOperationsItemType1Op,
    check_scim_groups_patch_body_operations_item_type_1_op,
)

T = TypeVar("T", bound="ScimGroupsPatchBodyOperationsItemType1")


@_attrs_define
class ScimGroupsPatchBodyOperationsItemType1:
    """
    Attributes:
        op (ScimGroupsPatchBodyOperationsItemType1Op): Operation type Example: remove.
        path (str): SCIM path for member to remove Example: members[value eq "550e8400-e29b-41d4-a716-446655440000"].
    """

    op: ScimGroupsPatchBodyOperationsItemType1Op
    path: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op: str = self.op

        path = self.path

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        op = check_scim_groups_patch_body_operations_item_type_1_op(d.pop("op"))

        path = d.pop("path")

        scim_groups_patch_body_operations_item_type_1 = cls(
            op=op,
            path=path,
        )

        scim_groups_patch_body_operations_item_type_1.additional_properties = d
        return scim_groups_patch_body_operations_item_type_1

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
