from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_user_patch_request_operations_item_op import (
    ScimUserPatchRequestOperationsItemOp,
    check_scim_user_patch_request_operations_item_op,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.scim_user_patch_request_operations_item_value_type_6 import (
        ScimUserPatchRequestOperationsItemValueType6,
    )


T = TypeVar("T", bound="ScimUserPatchRequestOperationsItem")


@_attrs_define
class ScimUserPatchRequestOperationsItem:
    """
    Attributes:
        op (ScimUserPatchRequestOperationsItemOp):
        value (bool | float | list[float] | list[str] | None | ScimUserPatchRequestOperationsItemValueType6 | str):
        path (str | Unset):
    """

    op: ScimUserPatchRequestOperationsItemOp
    value: bool | float | list[float] | list[str] | None | ScimUserPatchRequestOperationsItemValueType6 | str
    path: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.scim_user_patch_request_operations_item_value_type_6 import (
            ScimUserPatchRequestOperationsItemValueType6,
        )

        op: str = self.op

        value: bool | dict[str, Any] | float | list[float] | list[str] | None | str
        if isinstance(self.value, list):
            value = self.value

        elif isinstance(self.value, list):
            value = self.value

        elif isinstance(self.value, ScimUserPatchRequestOperationsItemValueType6):
            value = self.value.to_dict()
        else:
            value = self.value

        path = self.path

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "value": value,
            }
        )
        if path is not UNSET:
            field_dict["path"] = path

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_patch_request_operations_item_value_type_6 import (
            ScimUserPatchRequestOperationsItemValueType6,
        )

        d = dict(src_dict)
        op = check_scim_user_patch_request_operations_item_op(d.pop("op"))

        def _parse_value(
            data: object,
        ) -> bool | float | list[float] | list[str] | None | ScimUserPatchRequestOperationsItemValueType6 | str:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                value_type_2 = cast(list[str], data)

                return value_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, list):
                    raise TypeError()
                value_type_3 = cast(list[float], data)

                return value_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                value_type_6 = ScimUserPatchRequestOperationsItemValueType6.from_dict(data)

                return value_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                bool | float | list[float] | list[str] | None | ScimUserPatchRequestOperationsItemValueType6 | str, data
            )

        value = _parse_value(d.pop("value"))

        path = d.pop("path", UNSET)

        scim_user_patch_request_operations_item = cls(
            op=op,
            value=value,
            path=path,
        )

        scim_user_patch_request_operations_item.additional_properties = d
        return scim_user_patch_request_operations_item

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
