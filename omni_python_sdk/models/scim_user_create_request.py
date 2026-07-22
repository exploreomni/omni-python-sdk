from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.scim_user_create_request_urnomniparams_10_user_attribute import (
        ScimUserCreateRequestUrnomniparams10UserAttribute,
    )


T = TypeVar("T", bound="ScimUserCreateRequest")


@_attrs_define
class ScimUserCreateRequest:
    """
    Attributes:
        display_name (str): Display name of the user Example: John Doe.
        user_name (str): Email address (username) of the user Example: user@example.com.
        urnomniparams_1_0_user_attribute (ScimUserCreateRequestUrnomniparams10UserAttribute | Unset): Omni user
            attributes
    """

    display_name: str
    user_name: str
    urnomniparams_1_0_user_attribute: ScimUserCreateRequestUrnomniparams10UserAttribute | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        display_name = self.display_name

        user_name = self.user_name

        urnomniparams_1_0_user_attribute: dict[str, Any] | Unset = UNSET
        if not isinstance(self.urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = self.urnomniparams_1_0_user_attribute.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "displayName": display_name,
                "userName": user_name,
            }
        )
        if urnomniparams_1_0_user_attribute is not UNSET:
            field_dict["urn:omni:params:1.0:UserAttribute"] = urnomniparams_1_0_user_attribute

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_create_request_urnomniparams_10_user_attribute import (
            ScimUserCreateRequestUrnomniparams10UserAttribute,
        )

        d = dict(src_dict)
        display_name = d.pop("displayName")

        user_name = d.pop("userName")

        _urnomniparams_1_0_user_attribute = d.pop("urn:omni:params:1.0:UserAttribute", UNSET)
        urnomniparams_1_0_user_attribute: ScimUserCreateRequestUrnomniparams10UserAttribute | Unset
        if isinstance(_urnomniparams_1_0_user_attribute, Unset):
            urnomniparams_1_0_user_attribute = UNSET
        else:
            urnomniparams_1_0_user_attribute = ScimUserCreateRequestUrnomniparams10UserAttribute.from_dict(
                _urnomniparams_1_0_user_attribute
            )

        scim_user_create_request = cls(
            display_name=display_name,
            user_name=user_name,
            urnomniparams_1_0_user_attribute=urnomniparams_1_0_user_attribute,
        )

        scim_user_create_request.additional_properties = d
        return scim_user_create_request

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
