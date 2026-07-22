from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embed_sso_generate_session_body_user_attributes import EmbedSsoGenerateSessionBodyUserAttributes


T = TypeVar("T", bound="EmbedSsoGenerateSessionBody")


@_attrs_define
class EmbedSsoGenerateSessionBody:
    """
    Attributes:
        external_id (str): External identifier for the user (from your system) Example: user-123.
        name (str): Display name for the user Example: John Doe.
        groups (list[str] | Unset): Optional list of non-entity group names to assign to the user. Entity-group
            membership is managed by the entity parameter. Example: ['engineering', 'sales'].
        user_attributes (EmbedSsoGenerateSessionBodyUserAttributes | Unset): Optional user attributes for row-level
            security
    """

    external_id: str
    name: str
    groups: list[str] | Unset = UNSET
    user_attributes: EmbedSsoGenerateSessionBodyUserAttributes | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        external_id = self.external_id

        name = self.name

        groups: list[str] | Unset = UNSET
        if not isinstance(self.groups, Unset):
            groups = self.groups

        user_attributes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.user_attributes, Unset):
            user_attributes = self.user_attributes.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "externalId": external_id,
                "name": name,
            }
        )
        if groups is not UNSET:
            field_dict["groups"] = groups
        if user_attributes is not UNSET:
            field_dict["userAttributes"] = user_attributes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embed_sso_generate_session_body_user_attributes import EmbedSsoGenerateSessionBodyUserAttributes

        d = dict(src_dict)
        external_id = d.pop("externalId")

        name = d.pop("name")

        groups = cast(list[str], d.pop("groups", UNSET))

        _user_attributes = d.pop("userAttributes", UNSET)
        user_attributes: EmbedSsoGenerateSessionBodyUserAttributes | Unset
        if isinstance(_user_attributes, Unset):
            user_attributes = UNSET
        else:
            user_attributes = EmbedSsoGenerateSessionBodyUserAttributes.from_dict(_user_attributes)

        embed_sso_generate_session_body = cls(
            external_id=external_id,
            name=name,
            groups=groups,
            user_attributes=user_attributes,
        )

        embed_sso_generate_session_body.additional_properties = d
        return embed_sso_generate_session_body

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
