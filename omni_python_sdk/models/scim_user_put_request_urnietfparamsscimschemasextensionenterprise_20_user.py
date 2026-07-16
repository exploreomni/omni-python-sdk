from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6 import (
        ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6,
    )


T = TypeVar("T", bound="ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User")


@_attrs_define
class ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20User:
    """Enterprise SCIM user attributes"""

    additional_properties: dict[
        str,
        bool
        | float
        | list[float]
        | list[str]
        | None
        | ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
        | str,
    ] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6 import (
            ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6,
        )

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, list):
                field_dict[prop_name] = prop

            elif isinstance(prop, list):
                field_dict[prop_name] = prop

            elif isinstance(
                prop, ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
            ):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user_additional_property_type_6 import (
            ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6,
        )

        d = dict(src_dict)
        scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> (
                bool
                | float
                | list[float]
                | list[str]
                | None
                | ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
                | str
            ):
                if data is None:
                    return data
                try:
                    if not isinstance(data, list):
                        raise TypeError()
                    additional_property_type_2 = cast(list[str], data)

                    return additional_property_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, list):
                        raise TypeError()
                    additional_property_type_3 = cast(list[float], data)

                    return additional_property_type_3
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    additional_property_type_6 = ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6.from_dict(
                        data
                    )

                    return additional_property_type_6
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(
                    bool
                    | float
                    | list[float]
                    | list[str]
                    | None
                    | ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
                    | str,
                    data,
                )

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user.additional_properties = (
            additional_properties
        )
        return scim_user_put_request_urnietfparamsscimschemasextensionenterprise_20_user

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(
        self, key: str
    ) -> (
        bool
        | float
        | list[float]
        | list[str]
        | None
        | ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
        | str
    ):
        return self.additional_properties[key]

    def __setitem__(
        self,
        key: str,
        value: bool
        | float
        | list[float]
        | list[str]
        | None
        | ScimUserPutRequestUrnietfparamsscimschemasextensionenterprise20UserAdditionalPropertyType6
        | str,
    ) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
