from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_model_schema_base_access_grants_item_code_comments import (
        CreateModelSchemaBaseAccessGrantsItemCodeComments,
    )


T = TypeVar("T", bound="CreateModelSchemaBaseAccessGrantsItem")


@_attrs_define
class CreateModelSchemaBaseAccessGrantsItem:
    """
    Attributes:
        access_boostable (bool):
        name (str):
        allowed_values (list[str] | Unset):
        code_comments (CreateModelSchemaBaseAccessGrantsItemCodeComments | Unset):
        ignored (bool | Unset):
        user_attribute (str | Unset):
    """

    access_boostable: bool
    name: str
    allowed_values: list[str] | Unset = UNSET
    code_comments: CreateModelSchemaBaseAccessGrantsItemCodeComments | Unset = UNSET
    ignored: bool | Unset = UNSET
    user_attribute: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        access_boostable = self.access_boostable

        name = self.name

        allowed_values: list[str] | Unset = UNSET
        if not isinstance(self.allowed_values, Unset):
            allowed_values = self.allowed_values

        code_comments: dict[str, Any] | Unset = UNSET
        if not isinstance(self.code_comments, Unset):
            code_comments = self.code_comments.to_dict()

        ignored = self.ignored

        user_attribute = self.user_attribute

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "accessBoostable": access_boostable,
                "name": name,
            }
        )
        if allowed_values is not UNSET:
            field_dict["allowedValues"] = allowed_values
        if code_comments is not UNSET:
            field_dict["codeComments"] = code_comments
        if ignored is not UNSET:
            field_dict["ignored"] = ignored
        if user_attribute is not UNSET:
            field_dict["userAttribute"] = user_attribute

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_model_schema_base_access_grants_item_code_comments import (
            CreateModelSchemaBaseAccessGrantsItemCodeComments,
        )

        d = dict(src_dict)
        access_boostable = d.pop("accessBoostable")

        name = d.pop("name")

        allowed_values = cast(list[str], d.pop("allowedValues", UNSET))

        _code_comments = d.pop("codeComments", UNSET)
        code_comments: CreateModelSchemaBaseAccessGrantsItemCodeComments | Unset
        if isinstance(_code_comments, Unset):
            code_comments = UNSET
        else:
            code_comments = CreateModelSchemaBaseAccessGrantsItemCodeComments.from_dict(_code_comments)

        ignored = d.pop("ignored", UNSET)

        user_attribute = d.pop("userAttribute", UNSET)

        create_model_schema_base_access_grants_item = cls(
            access_boostable=access_boostable,
            name=name,
            allowed_values=allowed_values,
            code_comments=code_comments,
            ignored=ignored,
            user_attribute=user_attribute,
        )

        create_model_schema_base_access_grants_item.additional_properties = d
        return create_model_schema_base_access_grants_item

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
