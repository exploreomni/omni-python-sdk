from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.page_info import PageInfo


T = TypeVar("T", bound="DocumentsAccessListResponse")


@_attrs_define
class DocumentsAccessListResponse:
    """
    Attributes:
        page_info (PageInfo):
        principals (list[Any]): List of users and groups with access
    """

    page_info: PageInfo
    principals: list[Any]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        page_info = self.page_info.to_dict()

        principals = self.principals

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "pageInfo": page_info,
                "principals": principals,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.page_info import PageInfo

        d = dict(src_dict)
        page_info = PageInfo.from_dict(d.pop("pageInfo"))

        principals = cast(list[Any], d.pop("principals"))

        documents_access_list_response = cls(
            page_info=page_info,
            principals=principals,
        )

        documents_access_list_response.additional_properties = d
        return documents_access_list_response

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
