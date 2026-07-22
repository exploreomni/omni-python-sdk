from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.user_attributes_list_response_records_item_type import (
    UserAttributesListResponseRecordsItemType,
    check_user_attributes_list_response_records_item_type,
)

T = TypeVar("T", bound="UserAttributesListResponseRecordsItem")


@_attrs_define
class UserAttributesListResponseRecordsItem:
    """
    Attributes:
        default_value (float | list[float | str] | None | str): Default value applied when no user-specific value is
            set. When multiple_values is true, this is an array. Null if no default is configured. Example: us-east.
        description (None | str): Human-readable description of the attribute and its purpose Example: User region for
            row-level security filtering.
        id (str): Unique identifier for custom attributes. Empty string for system-defined attributes. Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        label (str): Display name shown in the Omni UI Example: Region.
        multiple_values (bool): Whether the attribute accepts an array of values. When true, default_value and user-
            specific values are arrays.
        name (str): Reference name used in model SQL and in embed SSO URL parameters Example: region.
        system (bool): System-defined attributes (e.g. omni_user_id, omni_user_email) are built-in and read-only. Custom
            attributes have system=false.
        type_ (UserAttributesListResponseRecordsItemType): Data type that determines valid values. String attributes
            accept text, Number attributes accept numeric values stored as strings for precision. Example: String.
    """

    default_value: float | list[float | str] | None | str
    description: None | str
    id: str
    label: str
    multiple_values: bool
    name: str
    system: bool
    type_: UserAttributesListResponseRecordsItemType
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        default_value: float | list[float | str] | None | str
        if isinstance(self.default_value, list):
            default_value = []
            for default_value_type_2_item_data in self.default_value:
                default_value_type_2_item: float | str
                default_value_type_2_item = default_value_type_2_item_data
                default_value.append(default_value_type_2_item)

        else:
            default_value = self.default_value

        description: None | str
        description = self.description

        id = self.id

        label = self.label

        multiple_values = self.multiple_values

        name = self.name

        system = self.system

        type_: str = self.type_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "default_value": default_value,
                "description": description,
                "id": id,
                "label": label,
                "multiple_values": multiple_values,
                "name": name,
                "system": system,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_default_value(data: object) -> float | list[float | str] | None | str:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                default_value_type_2 = []
                _default_value_type_2 = data
                for default_value_type_2_item_data in _default_value_type_2:

                    def _parse_default_value_type_2_item(data: object) -> float | str:
                        return cast(float | str, data)

                    default_value_type_2_item = _parse_default_value_type_2_item(default_value_type_2_item_data)

                    default_value_type_2.append(default_value_type_2_item)

                return default_value_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(float | list[float | str] | None | str, data)

        default_value = _parse_default_value(d.pop("default_value"))

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        id = d.pop("id")

        label = d.pop("label")

        multiple_values = d.pop("multiple_values")

        name = d.pop("name")

        system = d.pop("system")

        type_ = check_user_attributes_list_response_records_item_type(d.pop("type"))

        user_attributes_list_response_records_item = cls(
            default_value=default_value,
            description=description,
            id=id,
            label=label,
            multiple_values=multiple_values,
            name=name,
            system=system,
            type_=type_,
        )

        user_attributes_list_response_records_item.additional_properties = d
        return user_attributes_list_response_records_item

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
