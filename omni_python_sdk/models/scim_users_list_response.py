from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.scim_user_response import ScimUserResponse


T = TypeVar("T", bound="ScimUsersListResponse")


@_attrs_define
class ScimUsersListResponse:
    """
    Attributes:
        resources (list[ScimUserResponse]): List of SCIM users
        items_per_page (float): Items per page
        schemas (list[str]): SCIM schema URIs
        start_index (float): Start index (1-based)
        total_results (float): Total number of results
    """

    resources: list[ScimUserResponse]
    items_per_page: float
    schemas: list[str]
    start_index: float
    total_results: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resources = []
        for resources_item_data in self.resources:
            resources_item = resources_item_data.to_dict()
            resources.append(resources_item)

        items_per_page = self.items_per_page

        schemas = self.schemas

        start_index = self.start_index

        total_results = self.total_results

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "Resources": resources,
                "itemsPerPage": items_per_page,
                "schemas": schemas,
                "startIndex": start_index,
                "totalResults": total_results,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scim_user_response import ScimUserResponse

        d = dict(src_dict)
        resources = []
        _resources = d.pop("Resources")
        for resources_item_data in _resources:
            resources_item = ScimUserResponse.from_dict(resources_item_data)

            resources.append(resources_item)

        items_per_page = d.pop("itemsPerPage")

        schemas = cast(list[str], d.pop("schemas"))

        start_index = d.pop("startIndex")

        total_results = d.pop("totalResults")

        scim_users_list_response = cls(
            resources=resources,
            items_per_page=items_per_page,
            schemas=schemas,
            start_index=start_index,
            total_results=total_results,
        )

        scim_users_list_response.additional_properties = d
        return scim_users_list_response

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
