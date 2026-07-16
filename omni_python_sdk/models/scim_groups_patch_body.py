from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.scim_groups_patch_body_schemas_item import (
    ScimGroupsPatchBodySchemasItem,
    check_scim_groups_patch_body_schemas_item,
)

if TYPE_CHECKING:
    from ..models.scim_groups_patch_body_operations_item_type_0 import ScimGroupsPatchBodyOperationsItemType0
    from ..models.scim_groups_patch_body_operations_item_type_1 import ScimGroupsPatchBodyOperationsItemType1
    from ..models.scim_groups_patch_body_operations_item_type_2 import ScimGroupsPatchBodyOperationsItemType2
    from ..models.scim_groups_patch_body_operations_item_type_3 import ScimGroupsPatchBodyOperationsItemType3


T = TypeVar("T", bound="ScimGroupsPatchBody")


@_attrs_define
class ScimGroupsPatchBody:
    """
    Attributes:
        operations (list[ScimGroupsPatchBodyOperationsItemType0 | ScimGroupsPatchBodyOperationsItemType1 |
            ScimGroupsPatchBodyOperationsItemType2 | ScimGroupsPatchBodyOperationsItemType3]): List of SCIM patch operations
        schemas (list[ScimGroupsPatchBodySchemasItem]): SCIM schema URIs
    """

    operations: list[
        ScimGroupsPatchBodyOperationsItemType0
        | ScimGroupsPatchBodyOperationsItemType1
        | ScimGroupsPatchBodyOperationsItemType2
        | ScimGroupsPatchBodyOperationsItemType3
    ]
    schemas: list[ScimGroupsPatchBodySchemasItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.scim_groups_patch_body_operations_item_type_0 import ScimGroupsPatchBodyOperationsItemType0
        from ..models.scim_groups_patch_body_operations_item_type_1 import ScimGroupsPatchBodyOperationsItemType1
        from ..models.scim_groups_patch_body_operations_item_type_2 import ScimGroupsPatchBodyOperationsItemType2

        operations = []
        for operations_item_data in self.operations:
            operations_item: dict[str, Any]
            if isinstance(operations_item_data, ScimGroupsPatchBodyOperationsItemType0):
                operations_item = operations_item_data.to_dict()
            elif isinstance(operations_item_data, ScimGroupsPatchBodyOperationsItemType1):
                operations_item = operations_item_data.to_dict()
            elif isinstance(operations_item_data, ScimGroupsPatchBodyOperationsItemType2):
                operations_item = operations_item_data.to_dict()
            else:
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
        from ..models.scim_groups_patch_body_operations_item_type_0 import ScimGroupsPatchBodyOperationsItemType0
        from ..models.scim_groups_patch_body_operations_item_type_1 import ScimGroupsPatchBodyOperationsItemType1
        from ..models.scim_groups_patch_body_operations_item_type_2 import ScimGroupsPatchBodyOperationsItemType2
        from ..models.scim_groups_patch_body_operations_item_type_3 import ScimGroupsPatchBodyOperationsItemType3

        d = dict(src_dict)
        operations = []
        _operations = d.pop("Operations")
        for operations_item_data in _operations:

            def _parse_operations_item(
                data: object,
            ) -> (
                ScimGroupsPatchBodyOperationsItemType0
                | ScimGroupsPatchBodyOperationsItemType1
                | ScimGroupsPatchBodyOperationsItemType2
                | ScimGroupsPatchBodyOperationsItemType3
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    operations_item_type_0 = ScimGroupsPatchBodyOperationsItemType0.from_dict(data)

                    return operations_item_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    operations_item_type_1 = ScimGroupsPatchBodyOperationsItemType1.from_dict(data)

                    return operations_item_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    operations_item_type_2 = ScimGroupsPatchBodyOperationsItemType2.from_dict(data)

                    return operations_item_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                operations_item_type_3 = ScimGroupsPatchBodyOperationsItemType3.from_dict(data)

                return operations_item_type_3

            operations_item = _parse_operations_item(operations_item_data)

            operations.append(operations_item)

        schemas = []
        _schemas = d.pop("schemas")
        for schemas_item_data in _schemas:
            schemas_item = check_scim_groups_patch_body_schemas_item(schemas_item_data)

            schemas.append(schemas_item)

        scim_groups_patch_body = cls(
            operations=operations,
            schemas=schemas,
        )

        scim_groups_patch_body.additional_properties = d
        return scim_groups_patch_body

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
