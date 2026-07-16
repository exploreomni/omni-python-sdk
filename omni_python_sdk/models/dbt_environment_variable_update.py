from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DbtEnvironmentVariableUpdate")


@_attrs_define
class DbtEnvironmentVariableUpdate:
    """Update an existing variable by ID. Variable names cannot be changed after creation.

    Attributes:
        id (UUID): Existing variable ID
        is_secret (bool): Whether the variable value is secret
        value (None | str | Unset): Updated variable value. Omit or set to null to keep the existing value for secret
            variables.
    """

    id: UUID
    is_secret: bool
    value: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        is_secret = self.is_secret

        value: None | str | Unset
        if isinstance(self.value, Unset):
            value = UNSET
        else:
            value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "isSecret": is_secret,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = UUID(d.pop("id"))

        is_secret = d.pop("isSecret")

        def _parse_value(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        value = _parse_value(d.pop("value", UNSET))

        dbt_environment_variable_update = cls(
            id=id,
            is_secret=is_secret,
            value=value,
        )

        return dbt_environment_variable_update
