from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentFavoriteUser")


@_attrs_define
class DocumentFavoriteUser:
    """
    Attributes:
        email (None | str): Favoriting user's email. Null when the user has no resolvable email — e.g. an embed-SSO
            favoriter whose embed session did not provide one.
        favorited_at (str): ISO 8601 timestamp when the user favorited the document
        name (str): Favoriting user's display name
        user_id (str): Membership ID of the user who favorited the document (use with other v1 endpoints' userId
            parameter)
    """

    email: None | str
    favorited_at: str
    name: str
    user_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        email: None | str
        email = self.email

        favorited_at = self.favorited_at

        name = self.name

        user_id = self.user_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "email": email,
                "favoritedAt": favorited_at,
                "name": name,
                "userId": user_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_email(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        email = _parse_email(d.pop("email"))

        favorited_at = d.pop("favoritedAt")

        name = d.pop("name")

        user_id = d.pop("userId")

        document_favorite_user = cls(
            email=email,
            favorited_at=favorited_at,
            name=name,
            user_id=user_id,
        )

        document_favorite_user.additional_properties = d
        return document_favorite_user

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
