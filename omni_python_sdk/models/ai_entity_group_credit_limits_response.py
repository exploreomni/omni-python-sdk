from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.ai_entity_group_credit_limits_response_entity_groups_item import (
        AiEntityGroupCreditLimitsResponseEntityGroupsItem,
    )


T = TypeVar("T", bound="AiEntityGroupCreditLimitsResponse")


@_attrs_define
class AiEntityGroupCreditLimitsResponse:
    """
    Attributes:
        entity_groups (list[AiEntityGroupCreditLimitsResponseEntityGroupsItem]):
    """

    entity_groups: list[AiEntityGroupCreditLimitsResponseEntityGroupsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        entity_groups = []
        for entity_groups_item_data in self.entity_groups:
            entity_groups_item = entity_groups_item_data.to_dict()
            entity_groups.append(entity_groups_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "entityGroups": entity_groups,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_entity_group_credit_limits_response_entity_groups_item import (
            AiEntityGroupCreditLimitsResponseEntityGroupsItem,
        )

        d = dict(src_dict)
        entity_groups = []
        _entity_groups = d.pop("entityGroups")
        for entity_groups_item_data in _entity_groups:
            entity_groups_item = AiEntityGroupCreditLimitsResponseEntityGroupsItem.from_dict(entity_groups_item_data)

            entity_groups.append(entity_groups_item)

        ai_entity_group_credit_limits_response = cls(
            entity_groups=entity_groups,
        )

        ai_entity_group_credit_limits_response.additional_properties = d
        return ai_entity_group_credit_limits_response

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
