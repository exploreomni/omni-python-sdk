from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.models_list_response_records_item_branches_item import ModelsListResponseRecordsItemBranchesItem


T = TypeVar("T", bound="ModelsListResponseRecordsItem")


@_attrs_define
class ModelsListResponseRecordsItem:
    """
    Attributes:
        base_model_id (None | str): Base model ID for branch/extension models
        connection_id (None | str): Connection ID
        created_at (str): Creation timestamp
        deleted_at (None | str): Deletion timestamp
        id (str): Model ID
        model_kind (None | str): Model kind
        name (None | str): Model name
        updated_at (str): Last update timestamp
        branches (list[ModelsListResponseRecordsItemBranchesItem] | Unset): Active branches (if include=activeBranches)
    """

    base_model_id: None | str
    connection_id: None | str
    created_at: str
    deleted_at: None | str
    id: str
    model_kind: None | str
    name: None | str
    updated_at: str
    branches: list[ModelsListResponseRecordsItemBranchesItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_model_id: None | str
        base_model_id = self.base_model_id

        connection_id: None | str
        connection_id = self.connection_id

        created_at = self.created_at

        deleted_at: None | str
        deleted_at = self.deleted_at

        id = self.id

        model_kind: None | str
        model_kind = self.model_kind

        name: None | str
        name = self.name

        updated_at = self.updated_at

        branches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.branches, Unset):
            branches = []
            for branches_item_data in self.branches:
                branches_item = branches_item_data.to_dict()
                branches.append(branches_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseModelId": base_model_id,
                "connectionId": connection_id,
                "createdAt": created_at,
                "deletedAt": deleted_at,
                "id": id,
                "modelKind": model_kind,
                "name": name,
                "updatedAt": updated_at,
            }
        )
        if branches is not UNSET:
            field_dict["branches"] = branches

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_list_response_records_item_branches_item import ModelsListResponseRecordsItemBranchesItem

        d = dict(src_dict)

        def _parse_base_model_id(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        base_model_id = _parse_base_model_id(d.pop("baseModelId"))

        def _parse_connection_id(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        connection_id = _parse_connection_id(d.pop("connectionId"))

        created_at = d.pop("createdAt")

        def _parse_deleted_at(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        deleted_at = _parse_deleted_at(d.pop("deletedAt"))

        id = d.pop("id")

        def _parse_model_kind(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        model_kind = _parse_model_kind(d.pop("modelKind"))

        def _parse_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        name = _parse_name(d.pop("name"))

        updated_at = d.pop("updatedAt")

        _branches = d.pop("branches", UNSET)
        branches: list[ModelsListResponseRecordsItemBranchesItem] | Unset = UNSET
        if _branches is not UNSET:
            branches = []
            for branches_item_data in _branches:
                branches_item = ModelsListResponseRecordsItemBranchesItem.from_dict(branches_item_data)

                branches.append(branches_item)

        models_list_response_records_item = cls(
            base_model_id=base_model_id,
            connection_id=connection_id,
            created_at=created_at,
            deleted_at=deleted_at,
            id=id,
            model_kind=model_kind,
            name=name,
            updated_at=updated_at,
            branches=branches,
        )

        models_list_response_records_item.additional_properties = d
        return models_list_response_records_item

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
