from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_user_patch_request_schemas_item import (
    ScimUserPatchRequestSchemasItem,
    check_scim_user_patch_request_schemas_item,
)

if TYPE_CHECKING:
    from ..models.scim_user_patch_request_operations_item import ScimUserPatchRequestOperationsItem


T = TypeVar("T", bound="ScimUserPatchRequest")


@_attrs_define
class ScimUserPatchRequest:
    """
    Attributes:
        operations (list[ScimUserPatchRequestOperationsItem]): List of patch operations to apply
        schemas (list[ScimUserPatchRequestSchemasItem]): SCIM schema URIs
    """

    operations: list[ScimUserPatchRequestOperationsItem]
    schemas: list[ScimUserPatchRequestSchemasItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        operations = []
        for operations_item_data in self.operations:
            operations_item = operations_item_data.to_dict()
            operations.append(operations_item)

        schemas = []
        for schemas_item_data in self.schemas:
            schemas_item: str = schemas_item_data
            schemas.append(schemas_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "Operations": operations,
                "schemas": schemas,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_patch_request_operations_item import ScimUserPatchRequestOperationsItem

        d = dict(src_dict)
        operations = []
        _operations = d.pop("Operations")
        for operations_item_data in _operations:
            operations_item = ScimUserPatchRequestOperationsItem.from_dict(operations_item_data)

            operations.append(operations_item)

        schemas = []
        _schemas = d.pop("schemas")
        for schemas_item_data in _schemas:
            schemas_item = check_scim_user_patch_request_schemas_item(schemas_item_data)

            schemas.append(schemas_item)

        scim_user_patch_request = cls(
            operations=operations,
            schemas=schemas,
        )

        scim_user_patch_request.additional_properties = d
        return scim_user_patch_request

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
