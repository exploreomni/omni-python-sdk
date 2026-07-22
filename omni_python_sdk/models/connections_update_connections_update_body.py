from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.connections_update_connections_update_body_environment_user_attribute_type_0 import (
        ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0,
    )


T = TypeVar("T", bound="ConnectionsUpdateConnectionsUpdateBody")


@_attrs_define
class ConnectionsUpdateConnectionsUpdateBody:
    """Request body for updating connection attributes and credentials. At least one field must be provided.

    Attributes:
        base_role (str | Unset): Default role to assign to this connection Example: QUERIER.
        environment_user_attribute (ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0 | None | Unset):
            User attribute settings for connection environments
        password_unencrypted (str | Unset): New password or service account key. For BigQuery, this must be the JSON
            service account key file content.
        private_key (str | Unset): RSA private key for keypair authentication (Snowflake only). Must be PEM-encoded
            PKCS#8 format, minimum 2048-bit.
    """

    base_role: str | Unset = UNSET
    environment_user_attribute: ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0 | None | Unset = (
        UNSET
    )
    password_unencrypted: str | Unset = UNSET
    private_key: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.connections_update_connections_update_body_environment_user_attribute_type_0 import (
            ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0,
        )

        base_role = self.base_role

        environment_user_attribute: dict[str, Any] | None | Unset
        if isinstance(self.environment_user_attribute, Unset):
            environment_user_attribute = UNSET
        elif isinstance(
            self.environment_user_attribute, ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0
        ):
            environment_user_attribute = self.environment_user_attribute.to_dict()
        else:
            environment_user_attribute = self.environment_user_attribute

        password_unencrypted = self.password_unencrypted

        private_key = self.private_key

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if base_role is not UNSET:
            field_dict["baseRole"] = base_role
        if environment_user_attribute is not UNSET:
            field_dict["environmentUserAttribute"] = environment_user_attribute
        if password_unencrypted is not UNSET:
            field_dict["passwordUnencrypted"] = password_unencrypted
        if private_key is not UNSET:
            field_dict["privateKey"] = private_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connections_update_connections_update_body_environment_user_attribute_type_0 import (
            ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0,
        )

        d = dict(src_dict)
        base_role = d.pop("baseRole", UNSET)

        def _parse_environment_user_attribute(
            data: object,
        ) -> ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                environment_user_attribute_type_0 = (
                    ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0.from_dict(data)
                )

                return environment_user_attribute_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0 | None | Unset, data)

        environment_user_attribute = _parse_environment_user_attribute(d.pop("environmentUserAttribute", UNSET))

        password_unencrypted = d.pop("passwordUnencrypted", UNSET)

        private_key = d.pop("privateKey", UNSET)

        connections_update_connections_update_body = cls(
            base_role=base_role,
            environment_user_attribute=environment_user_attribute,
            password_unencrypted=password_unencrypted,
            private_key=private_key,
        )

        connections_update_connections_update_body.additional_properties = d
        return connections_update_connections_update_body

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
