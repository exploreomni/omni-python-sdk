from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.users_create_email_only_bulk_response_results_item import UsersCreateEmailOnlyBulkResponseResultsItem


T = TypeVar("T", bound="UsersCreateEmailOnlyBulkResponse")


@_attrs_define
class UsersCreateEmailOnlyBulkResponse:
    """
    Attributes:
        results (list[UsersCreateEmailOnlyBulkResponseResultsItem]): Results for each created user
    """

    results: list[UsersCreateEmailOnlyBulkResponseResultsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        results = []
        for results_item_data in self.results:
            results_item = results_item_data.to_dict()
            results.append(results_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "results": results,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.users_create_email_only_bulk_response_results_item import (
            UsersCreateEmailOnlyBulkResponseResultsItem,
        )

        d = dict(src_dict)
        results = []
        _results = d.pop("results")
        for results_item_data in _results:
            results_item = UsersCreateEmailOnlyBulkResponseResultsItem.from_dict(results_item_data)

            results.append(results_item)

        users_create_email_only_bulk_response = cls(
            results=results,
        )

        users_create_email_only_bulk_response.additional_properties = d
        return users_create_email_only_bulk_response

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
