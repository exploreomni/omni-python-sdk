from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.scim_user_patch_request_operations_item_value_type_6_urnietfparamsscimschemasextensionenterprise_20_user import (
        ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User,
    )
    from ..models.scim_user_patch_request_operations_item_value_type_6_urnomniparams_10_user_attribute import (
        ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute,
    )


T = TypeVar("T", bound="ScimUserPatchRequestOperationsItemValueType6")


@_attrs_define
class ScimUserPatchRequestOperationsItemValueType6:
    """
    Attributes:
        urnietfparamsscimschemasextensionenterprise_2_0_user
            (ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User | Unset):
        urnomniparams_1_0_user_attribute (ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute |
            Unset):
        active (bool | Unset):
        display_name (str | Unset):
        user_name (str | Unset):
    """

    urnietfparamsscimschemasextensionenterprise_2_0_user: (
        ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User | Unset
    ) = UNSET
    urnomniparams_1_0_user_attribute: (
        ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute | Unset
    ) = UNSET
    active: bool | Unset = UNSET
    display_name: str | Unset = UNSET
    user_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        urnietfparamsscimschemasextensionenterprise_2_0_user: dict[str, Any] | Unset = UNSET
        if not isinstance(self.urnietfparamsscimschemasextensionenterprise_2_0_user, Unset):
            urnietfparamsscimschemasextensionenterprise_2_0_user = (
                self.urnietfparamsscimschemasextensionenterprise_2_0_user.to_dict()
            )

        urnomniparams_1_0_user_attribute: dict[str, Any] | Unset = UNSET
        if not isinstance(self.urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = self.urnomniparams_1_0_user_attribute.to_dict()

        active = self.active

        display_name = self.display_name

        user_name = self.user_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if urnietfparamsscimschemasextensionenterprise_2_0_user is not UNSET:
            field_dict["urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"] = (
                urnietfparamsscimschemasextensionenterprise_2_0_user
            )
        if urnomniparams_1_0_user_attribute is not UNSET:
            field_dict["urn:omni:params:1.0:UserAttribute"] = urnomniparams_1_0_user_attribute
        if active is not UNSET:
            field_dict["active"] = active
        if display_name is not UNSET:
            field_dict["displayName"] = display_name
        if user_name is not UNSET:
            field_dict["userName"] = user_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_patch_request_operations_item_value_type_6_urnietfparamsscimschemasextensionenterprise_20_user import (
            ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User,
        )
        from ..models.scim_user_patch_request_operations_item_value_type_6_urnomniparams_10_user_attribute import (
            ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute,
        )

        d = dict(src_dict)
        _urnietfparamsscimschemasextensionenterprise_2_0_user = d.pop(
            "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User", UNSET
        )
        urnietfparamsscimschemasextensionenterprise_2_0_user: (
            ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User | Unset
        )
        if isinstance(_urnietfparamsscimschemasextensionenterprise_2_0_user, Unset):
            urnietfparamsscimschemasextensionenterprise_2_0_user = UNSET
        else:
            urnietfparamsscimschemasextensionenterprise_2_0_user = (
                ScimUserPatchRequestOperationsItemValueType6Urnietfparamsscimschemasextensionenterprise20User.from_dict(
                    _urnietfparamsscimschemasextensionenterprise_2_0_user
                )
            )

        _urnomniparams_1_0_user_attribute = d.pop("urn:omni:params:1.0:UserAttribute", UNSET)
        urnomniparams_1_0_user_attribute: (
            ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute | Unset
        )
        if isinstance(_urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = UNSET
        else:
            urnomniparams_1_0_user_attribute = (
                ScimUserPatchRequestOperationsItemValueType6Urnomniparams10UserAttribute.from_dict(
                    _urnomniparams_1_0_user_attribute
                )
            )

        active = d.pop("active", UNSET)

        display_name = d.pop("displayName", UNSET)

        user_name = d.pop("userName", UNSET)

        scim_user_patch_request_operations_item_value_type_6 = cls(
            urnietfparamsscimschemasextensionenterprise_2_0_user=urnietfparamsscimschemasextensionenterprise_2_0_user,
            urnomniparams_1_0_user_attribute=urnomniparams_1_0_user_attribute,
            active=active,
            display_name=display_name,
            user_name=user_name,
        )

        scim_user_patch_request_operations_item_value_type_6.additional_properties = d
        return scim_user_patch_request_operations_item_value_type_6

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
