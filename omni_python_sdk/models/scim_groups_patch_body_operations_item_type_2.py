from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_groups_patch_body_operations_item_type_2_op import (
    ScimGroupsPatchBodyOperationsItemType2Op,
    check_scim_groups_patch_body_operations_item_type_2_op,
)
from ..models.scim_groups_patch_body_operations_item_type_2_path import (
    ScimGroupsPatchBodyOperationsItemType2Path,
    check_scim_groups_patch_body_operations_item_type_2_path,
)

if TYPE_CHECKING:
    from ..models.scim_groups_patch_body_operations_item_type_2_value_item import (
        ScimGroupsPatchBodyOperationsItemType2ValueItem,
    )


T = TypeVar("T", bound="ScimGroupsPatchBodyOperationsItemType2")


@_attrs_define
class ScimGroupsPatchBodyOperationsItemType2:
    """
    Attributes:
        op (ScimGroupsPatchBodyOperationsItemType2Op): Operation type Example: add.
        path (ScimGroupsPatchBodyOperationsItemType2Path): Path for members Example: members.
        value (list[ScimGroupsPatchBodyOperationsItemType2ValueItem]):
    """

    op: ScimGroupsPatchBodyOperationsItemType2Op
    path: ScimGroupsPatchBodyOperationsItemType2Path
    value: list[ScimGroupsPatchBodyOperationsItemType2ValueItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op: str = self.op

        path: str = self.path

        value = []
        for value_item_data in self.value:
            value_item = value_item_data.to_dict()
            value.append(value_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "path": path,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_groups_patch_body_operations_item_type_2_value_item import (
            ScimGroupsPatchBodyOperationsItemType2ValueItem,
        )

        d = dict(src_dict)
        op = check_scim_groups_patch_body_operations_item_type_2_op(d.pop("op"))

        path = check_scim_groups_patch_body_operations_item_type_2_path(d.pop("path"))

        value = []
        _value = d.pop("value")
        for value_item_data in _value:
            value_item = ScimGroupsPatchBodyOperationsItemType2ValueItem.from_dict(value_item_data)

            value.append(value_item)

        scim_groups_patch_body_operations_item_type_2 = cls(
            op=op,
            path=path,
            value=value,
        )

        scim_groups_patch_body_operations_item_type_2.additional_properties = d
        return scim_groups_patch_body_operations_item_type_2

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
