from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_groups_patch_body_operations_item_type_3_op import (
    ScimGroupsPatchBodyOperationsItemType3Op,
    check_scim_groups_patch_body_operations_item_type_3_op,
)
from ..models.scim_groups_patch_body_operations_item_type_3_path import (
    ScimGroupsPatchBodyOperationsItemType3Path,
    check_scim_groups_patch_body_operations_item_type_3_path,
)

if TYPE_CHECKING:
    from ..models.scim_groups_patch_body_operations_item_type_3_value_type_0_item import (
        ScimGroupsPatchBodyOperationsItemType3ValueType0Item,
    )


T = TypeVar("T", bound="ScimGroupsPatchBodyOperationsItemType3")


@_attrs_define
class ScimGroupsPatchBodyOperationsItemType3:
    """
    Attributes:
        op (ScimGroupsPatchBodyOperationsItemType3Op): Operation type Example: replace.
        path (ScimGroupsPatchBodyOperationsItemType3Path): Path for attribute to replace Example: members.
        value (list[ScimGroupsPatchBodyOperationsItemType3ValueType0Item] | str):
    """

    op: ScimGroupsPatchBodyOperationsItemType3Op
    path: ScimGroupsPatchBodyOperationsItemType3Path
    value: list[ScimGroupsPatchBodyOperationsItemType3ValueType0Item] | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op: str = self.op

        path: str = self.path

        value: list[dict[str, Any]] | str
        if isinstance(self.value, list):
            value = []
            for value_type_0_item_data in self.value:
                value_type_0_item = value_type_0_item_data.to_dict()
                value.append(value_type_0_item)

        else:
            value = self.value

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
        from ..models.scim_groups_patch_body_operations_item_type_3_value_type_0_item import (
            ScimGroupsPatchBodyOperationsItemType3ValueType0Item,
        )

        d = dict(src_dict)
        op = check_scim_groups_patch_body_operations_item_type_3_op(d.pop("op"))

        path = check_scim_groups_patch_body_operations_item_type_3_path(d.pop("path"))

        def _parse_value(data: object) -> list[ScimGroupsPatchBodyOperationsItemType3ValueType0Item] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                value_type_0 = []
                _value_type_0 = data
                for value_type_0_item_data in _value_type_0:
                    value_type_0_item = ScimGroupsPatchBodyOperationsItemType3ValueType0Item.from_dict(
                        value_type_0_item_data
                    )

                    value_type_0.append(value_type_0_item)

                return value_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ScimGroupsPatchBodyOperationsItemType3ValueType0Item] | str, data)

        value = _parse_value(d.pop("value"))

        scim_groups_patch_body_operations_item_type_3 = cls(
            op=op,
            path=path,
            value=value,
        )

        scim_groups_patch_body_operations_item_type_3.additional_properties = d
        return scim_groups_patch_body_operations_item_type_3

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
