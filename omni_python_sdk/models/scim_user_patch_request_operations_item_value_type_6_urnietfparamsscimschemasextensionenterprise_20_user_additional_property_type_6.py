from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar(
    "T",
    bound="ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6",
)


@_attrs_define
class ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6:
    """ """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scim_user_patch_request_operations_item_value_type_6_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6 = cls()

        scim_user_patch_request_operations_item_value_type_6_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6.additional_properties = d
        return scim_user_patch_request_operations_item_value_type_6_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6

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
