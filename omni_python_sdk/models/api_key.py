from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.api_key_type import ApiKeyType, check_api_key_type

T = TypeVar("T", bound="ApiKey")


@_attrs_define
class ApiKey:
    """
    Attributes:
        created_at (datetime.datetime): ISO 8601 timestamp of when the token was created Example:
            2026-01-15T10:00:00.000Z.
        enabled (bool): Whether the token can currently authenticate. A disabled token cannot authenticate but remains
            visible until deleted. Example: True.
        id (UUID): Unique identifier for the token Example: a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        membership_id (None | UUID): Membership ID of the user the token is scoped to. Null for organization-level
            tokens. Example: b2c3d4e5-f6a7-8901-bcde-f12345678901.
        name (str): Human-readable name for the token Example: CI deployment key.
        type_ (ApiKeyType): Token type: `organization` (org-level), `personal` (user-created personal access token), or
            `mcp` (MCP OAuth grant). Example: organization.
    """

    created_at: datetime.datetime
    enabled: bool
    id: UUID
    membership_id: None | UUID
    name: str
    type_: ApiKeyType
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created_at = self.created_at.isoformat()

        enabled = self.enabled

        id = str(self.id)

        membership_id: None | str
        if isinstance(self.membership_id, UUID):
            membership_id = str(self.membership_id)
        else:
            membership_id = self.membership_id

        name = self.name

        type_: str = self.type_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "createdAt": created_at,
                "enabled": enabled,
                "id": id,
                "membershipId": membership_id,
                "name": name,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        enabled = d.pop("enabled")

        id = UUID(d.pop("id"))

        def _parse_membership_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                membership_id_type_0 = UUID(data)

                return membership_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        membership_id = _parse_membership_id(d.pop("membershipId"))

        name = d.pop("name")

        type_ = check_api_key_type(d.pop("type"))

        api_key = cls(
            created_at=created_at,
            enabled=enabled,
            id=id,
            membership_id=membership_id,
            name=name,
            type_=type_,
        )

        api_key.additional_properties = d
        return api_key

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
