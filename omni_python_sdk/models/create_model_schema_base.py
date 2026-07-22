from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.create_model_schema_base_model_kind_type_0 import (
    CreateModelSchemaBaseModelKindType0,
    check_create_model_schema_base_model_kind_type_0,
)
from ..models.create_model_schema_base_model_kind_type_1 import (
    CreateModelSchemaBaseModelKindType1,
    check_create_model_schema_base_model_kind_type_1,
)
from ..models.create_model_schema_base_model_kind_type_2 import (
    CreateModelSchemaBaseModelKindType2,
    check_create_model_schema_base_model_kind_type_2,
)
from ..models.create_model_schema_base_model_kind_type_3 import (
    CreateModelSchemaBaseModelKindType3,
    check_create_model_schema_base_model_kind_type_3,
)
from ..models.create_model_schema_base_model_kind_type_4 import (
    CreateModelSchemaBaseModelKindType4,
    check_create_model_schema_base_model_kind_type_4,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_model_schema_base_access_grants_item import CreateModelSchemaBaseAccessGrantsItem


T = TypeVar("T", bound="CreateModelSchemaBase")


@_attrs_define
class CreateModelSchemaBase:
    """
    Attributes:
        connection_id (str): Connection ID for the model
        access_grants (list[CreateModelSchemaBaseAccessGrantsItem] | Unset): Access grants for the model
        allow_as_workbook_base (bool | Unset): Allow this model as a workbook base
        base_model_id (str | Unset): Base model ID for extension or branch models
        model_kind (CreateModelSchemaBaseModelKindType0 | CreateModelSchemaBaseModelKindType1 |
            CreateModelSchemaBaseModelKindType2 | CreateModelSchemaBaseModelKindType3 | CreateModelSchemaBaseModelKindType4
            | Unset): Kind of model to create Default: 'SCHEMA'.
        model_name (str | Unset): Name for the model
        uses_isolated_branches (bool | Unset): For SHARED_EXTENSION models, controls if branches are shown on extension
            model page instead of parent shared model
    """

    connection_id: str
    access_grants: list[CreateModelSchemaBaseAccessGrantsItem] | Unset = UNSET
    allow_as_workbook_base: bool | Unset = UNSET
    base_model_id: str | Unset = UNSET
    model_kind: (
        CreateModelSchemaBaseModelKindType0
        | CreateModelSchemaBaseModelKindType1
        | CreateModelSchemaBaseModelKindType2
        | CreateModelSchemaBaseModelKindType3
        | CreateModelSchemaBaseModelKindType4
        | Unset
    ) = "SCHEMA"
    model_name: str | Unset = UNSET
    uses_isolated_branches: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_id = self.connection_id

        access_grants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.access_grants, Unset):
            access_grants = []
            for access_grants_item_data in self.access_grants:
                access_grants_item = access_grants_item_data.to_dict()
                access_grants.append(access_grants_item)

        allow_as_workbook_base = self.allow_as_workbook_base

        base_model_id = self.base_model_id

        model_kind: str | Unset
        if isinstance(self.model_kind, Unset):
            model_kind = UNSET
        elif isinstance(self.model_kind, str):
            model_kind = self.model_kind
        elif isinstance(self.model_kind, str):
            model_kind = self.model_kind
        elif isinstance(self.model_kind, str):
            model_kind = self.model_kind
        elif isinstance(self.model_kind, str):
            model_kind = self.model_kind
        else:
            model_kind = self.model_kind

        model_name = self.model_name

        uses_isolated_branches = self.uses_isolated_branches

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionId": connection_id,
            }
        )
        if access_grants is not UNSET:
            field_dict["accessGrants"] = access_grants
        if allow_as_workbook_base is not UNSET:
            field_dict["allowAsWorkbookBase"] = allow_as_workbook_base
        if base_model_id is not UNSET:
            field_dict["baseModelId"] = base_model_id
        if model_kind is not UNSET:
            field_dict["modelKind"] = model_kind
        if model_name is not UNSET:
            field_dict["modelName"] = model_name
        if uses_isolated_branches is not UNSET:
            field_dict["usesIsolatedBranches"] = uses_isolated_branches

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_model_schema_base_access_grants_item import CreateModelSchemaBaseAccessGrantsItem

        d = dict(src_dict)
        connection_id = d.pop("connectionId")

        _access_grants = d.pop("accessGrants", UNSET)
        access_grants: list[CreateModelSchemaBaseAccessGrantsItem] | Unset = UNSET
        if _access_grants is not UNSET:
            access_grants = []
            for access_grants_item_data in _access_grants:
                access_grants_item = CreateModelSchemaBaseAccessGrantsItem.from_dict(access_grants_item_data)

                access_grants.append(access_grants_item)

        allow_as_workbook_base = d.pop("allowAsWorkbookBase", UNSET)

        base_model_id = d.pop("baseModelId", UNSET)

        def _parse_model_kind(
            data: object,
        ) -> (
            CreateModelSchemaBaseModelKindType0
            | CreateModelSchemaBaseModelKindType1
            | CreateModelSchemaBaseModelKindType2
            | CreateModelSchemaBaseModelKindType3
            | CreateModelSchemaBaseModelKindType4
            | Unset
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_kind_type_0 = check_create_model_schema_base_model_kind_type_0(data)

                return model_kind_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_kind_type_1 = check_create_model_schema_base_model_kind_type_1(data)

                return model_kind_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_kind_type_2 = check_create_model_schema_base_model_kind_type_2(data)

                return model_kind_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_kind_type_3 = check_create_model_schema_base_model_kind_type_3(data)

                return model_kind_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, str):
                raise TypeError()
            model_kind_type_4 = check_create_model_schema_base_model_kind_type_4(data)

            return model_kind_type_4

        model_kind = _parse_model_kind(d.pop("modelKind", UNSET))

        model_name = d.pop("modelName", UNSET)

        uses_isolated_branches = d.pop("usesIsolatedBranches", UNSET)

        create_model_schema_base = cls(
            connection_id=connection_id,
            access_grants=access_grants,
            allow_as_workbook_base=allow_as_workbook_base,
            base_model_id=base_model_id,
            model_kind=model_kind,
            model_name=model_name,
            uses_isolated_branches=uses_isolated_branches,
        )

        create_model_schema_base.additional_properties = d
        return create_model_schema_base

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
