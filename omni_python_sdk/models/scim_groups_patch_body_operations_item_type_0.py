from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_groups_patch_body_operations_item_type_0_op import (
    ScimGroupsPatchBodyOperationsItemType0Op,
    check_scim_groups_patch_body_operations_item_type_0_op,
)

if TYPE_CHECKING:
    from ..models.scim_groups_patch_body_operations_item_type_0_value import ScimGroupsPatchBodyOperationsItemType0Value


T = TypeVar("T", bound="ScimGroupsPatchBodyOperationsItemType0")


@_attrs_define
class ScimGroupsPatchBodyOperationsItemType0:
    """
    Attributes:
        op (ScimGroupsPatchBodyOperationsItemType0Op): Operation type Example: replace.
        value (ScimGroupsPatchBodyOperationsItemType0Value):
    """

    op: ScimGroupsPatchBodyOperationsItemType0Op
    value: ScimGroupsPatchBodyOperationsItemType0Value
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op: str = self.op

        value = self.value.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_groups_patch_body_operations_item_type_0_value import (
            ScimGroupsPatchBodyOperationsItemType0Value,
        )

        d = dict(src_dict)
        op = check_scim_groups_patch_body_operations_item_type_0_op(d.pop("op"))

        value = ScimGroupsPatchBodyOperationsItemType0Value.from_dict(d.pop("value"))

        scim_groups_patch_body_operations_item_type_0 = cls(
            op=op,
            value=value,
        )

        scim_groups_patch_body_operations_item_type_0.additional_properties = d
        return scim_groups_patch_body_operations_item_type_0

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
