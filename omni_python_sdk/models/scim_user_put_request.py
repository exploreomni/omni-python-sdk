from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user import (
        ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User,
    )
    from ..models.scim_user_put_request_urnomniparams_10_user_attribute import (
        ScimUserPutRequestUrnomniparams10UserAttribute,
    )


T = TypeVar("T", bound="ScimUserPutRequest")


@_attrs_define
class ScimUserPutRequest:
    """
    Attributes:
        user_name (str): Email address (username) of the user Example: user@example.com.
        active (bool | Unset): Whether the user is active Default: True.
        display_name (str | Unset): Display name of the user
        urnietfparamsscimschemasextensionenterprise_2_0_user
            (ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User | Unset): Enterprise SCIM user attributes
        urnomniparams_1_0_user_attribute (ScimUserPutRequestUrnomniparams10UserAttribute | Unset): Omni user attributes
    """

    user_name: str
    active: bool | Unset = True
    display_name: str | Unset = UNSET
    urnietfparamsscimschemasextensionenterprise_2_0_user: (
        ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User | Unset
    ) = UNSET
    urnomniparams_1_0_user_attribute: ScimUserPutRequestUrnomniparams10UserAttribute | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        user_name = self.user_name

        active = self.active

        display_name = self.display_name

        urnietfparamsscimschemasextensionenterprise_2_0_user: dict[str, Any] | Unset = UNSET
        if not isinstance(self.urnietfparamsscimschemasextensionenterprise_2_0_user, Unset):
            urnietfparamsscimschemasextensionenterprise_2_0_user = (
                self.urnietfparamsscimschemasextensionenterprise_2_0_user.to_dict()
            )

        urnomniparams_1_0_user_attribute: dict[str, Any] | Unset = UNSET
        if not isinstance(self.urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = self.urnomniparams_1_0_user_attribute.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "userName": user_name,
            }
        )
        if active is not UNSET:
            field_dict["active"] = active
        if display_name is not UNSET:
            field_dict["displayName"] = display_name
        if urnietfparamsscimschemasextensionenterprise_2_0_user is not UNSET:
            field_dict["urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"] = (
                urnietfparamsscimschemasextensionenterprise_2_0_user
            )
        if urnomniparams_1_0_user_attribute is not UNSET:
            field_dict["urn:omni:params:1.0:UserAttribute"] = urnomniparams_1_0_user_attribute

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user import (
            ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User,
        )
        from ..models.scim_user_put_request_urnomniparams_10_user_attribute import (
            ScimUserPutRequestUrnomniparams10UserAttribute,
        )

        d = dict(src_dict)
        user_name = d.pop("userName")

        active = d.pop("active", UNSET)

        display_name = d.pop("displayName", UNSET)

        _urnietfparamsscimschemasextensionenterprise_2_0_user = d.pop(
            "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User", UNSET
        )
        urnietfparamsscimschemasextensionenterprise_2_0_user: (
            ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User | Unset
        )
        if isinstance(_urnietfparamsscimschemasextensionenterprise_2_0_user, Unset):
            urnietfparamsscimschemasextensionenterprise_2_0_user = UNSET
        else:
            urnietfparamsscimschemasextensionenterprise_2_0_user = (
                ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User.from_dict(
                    _urnietfparamsscimschemasextensionenterprise_2_0_user
                )
            )

        _urnomniparams_1_0_user_attribute = d.pop("urn:omni:params:1.0:UserAttribute", UNSET)
        urnomniparams_1_0_user_attribute: ScimUserPutRequestUrnomniparams10UserAttribute | Unset
        if isinstance(_urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = UNSET
        else:
            urnomniparams_1_0_user_attribute = ScimUserPutRequestUrnomniparams10UserAttribute.from_dict(
                _urnomniparams_1_0_user_attribute
            )

        scim_user_put_request = cls(
            user_name=user_name,
            active=active,
            display_name=display_name,
            urnietfparamsscimschemasextensionenterprise_2_0_user=urnietfparamsscimschemasextensionenterprise_2_0_user,
            urnomniparams_1_0_user_attribute=urnomniparams_1_0_user_attribute,
        )

        scim_user_put_request.additional_properties = d
        return scim_user_put_request

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
