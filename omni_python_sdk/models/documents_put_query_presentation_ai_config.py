from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.documents_put_query_presentation_ai_config_description import (
        DocumentsPutQueryPresentationAiConfigDescription,
    )
    from ..models.documents_put_query_presentation_ai_config_sub_title import (
        DocumentsPutQueryPresentationAiConfigSubTitle,
    )


T = TypeVar("T", bound="DocumentsPutQueryPresentationAiConfig")


@_attrs_define
class DocumentsPutQueryPresentationAiConfig:
    """AI configuration

    Attributes:
        description (DocumentsPutQueryPresentationAiConfigDescription | Unset):
        sub_title (DocumentsPutQueryPresentationAiConfigSubTitle | Unset):
    """

    description: DocumentsPutQueryPresentationAiConfigDescription | Unset = UNSET
    sub_title: DocumentsPutQueryPresentationAiConfigSubTitle | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description: dict[str, Any] | Unset = UNSET
        if not isinstance(self.description, Unset):
            description = self.description.to_dict()

        sub_title: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sub_title, Unset):
            sub_title = self.sub_title.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if sub_title is not UNSET:
            field_dict["subTitle"] = sub_title

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.documents_put_query_presentation_ai_config_description import (
            DocumentsPutQueryPresentationAiConfigDescription,
        )
        from ..models.documents_put_query_presentation_ai_config_sub_title import (
            DocumentsPutQueryPresentationAiConfigSubTitle,
        )

        d = dict(src_dict)
        _description = d.pop("description", UNSET)
        description: DocumentsPutQueryPresentationAiConfigDescription | Unset
        if isinstance(_description, Unset):
            description = UNSET
        else:
            description = DocumentsPutQueryPresentationAiConfigDescription.from_dict(_description)

        _sub_title = d.pop("subTitle", UNSET)
        sub_title: DocumentsPutQueryPresentationAiConfigSubTitle | Unset
        if isinstance(_sub_title, Unset):
            sub_title = UNSET
        else:
            sub_title = DocumentsPutQueryPresentationAiConfigSubTitle.from_dict(_sub_title)

        documents_put_query_presentation_ai_config = cls(
            description=description,
            sub_title=sub_title,
        )

        documents_put_query_presentation_ai_config.additional_properties = d
        return documents_put_query_presentation_ai_config

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
