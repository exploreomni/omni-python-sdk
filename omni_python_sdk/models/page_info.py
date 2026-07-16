from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="PageInfo")


@_attrs_define
class PageInfo:
    """
    Attributes:
        has_next_page (bool): Whether more results are available
        next_cursor (None | str): Cursor for fetching the next page
        page_size (float): Number of results per page
        total_records (float): Total number of records matching the query
    """

    has_next_page: bool
    next_cursor: None | str
    page_size: float
    total_records: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        has_next_page = self.has_next_page

        next_cursor: None | str
        next_cursor = self.next_cursor

        page_size = self.page_size

        total_records = self.total_records

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "hasNextPage": has_next_page,
                "nextCursor": next_cursor,
                "pageSize": page_size,
                "totalRecords": total_records,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        has_next_page = d.pop("hasNextPage")

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("nextCursor"))

        page_size = d.pop("pageSize")

        total_records = d.pop("totalRecords")

        page_info = cls(
            has_next_page=has_next_page,
            next_cursor=next_cursor,
            page_size=page_size,
            total_records=total_records,
        )

        page_info.additional_properties = d
        return page_info

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
