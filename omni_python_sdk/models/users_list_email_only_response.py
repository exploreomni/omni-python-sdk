from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.page_info import PageInfo
    from ..models.users_list_email_only_response_records_item import UsersListEmailOnlyResponseRecordsItem


T = TypeVar("T", bound="UsersListEmailOnlyResponse")


@_attrs_define
class UsersListEmailOnlyResponse:
    """
    Attributes:
        page_info (PageInfo):
        records (list[UsersListEmailOnlyResponseRecordsItem]):
    """

    page_info: PageInfo
    records: list[UsersListEmailOnlyResponseRecordsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        page_info = self.page_info.to_dict()

        records = []
        for records_item_data in self.records:
            records_item = records_item_data.to_dict()
            records.append(records_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "pageInfo": page_info,
                "records": records,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.page_info import PageInfo
        from ..models.users_list_email_only_response_records_item import UsersListEmailOnlyResponseRecordsItem

        d = dict(src_dict)
        page_info = PageInfo.from_dict(d.pop("pageInfo"))

        records = []
        _records = d.pop("records")
        for records_item_data in _records:
            records_item = UsersListEmailOnlyResponseRecordsItem.from_dict(records_item_data)

            records.append(records_item)

        users_list_email_only_response = cls(
            page_info=page_info,
            records=records,
        )

        users_list_email_only_response.additional_properties = d
        return users_list_email_only_response

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
