from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.ai_entity_group_credit_limit_entry import AiEntityGroupCreditLimitEntry


T = TypeVar("T", bound="AiEntityGroupCreditLimitsUpdateBody")


@_attrs_define
class AiEntityGroupCreditLimitsUpdateBody:
    """
    Attributes:
        entity_groups (list[AiEntityGroupCreditLimitEntry]): Entity groups to update, at most 1000 per request. Each
            entry has an `entity` plus exactly one of `creditLimit` (number or `null`) or `useDefaultLimit: true`.
    """

    entity_groups: list[AiEntityGroupCreditLimitEntry]

    def to_dict(self) -> dict[str, Any]:
        entity_groups = []
        for entity_groups_item_data in self.entity_groups:
            entity_groups_item = entity_groups_item_data.to_dict()
            entity_groups.append(entity_groups_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entityGroups": entity_groups,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_entity_group_credit_limit_entry import AiEntityGroupCreditLimitEntry

        d = dict(src_dict)
        entity_groups = []
        _entity_groups = d.pop("entityGroups")
        for entity_groups_item_data in _entity_groups:
            entity_groups_item = AiEntityGroupCreditLimitEntry.from_dict(entity_groups_item_data)

            entity_groups.append(entity_groups_item)

        ai_entity_group_credit_limits_update_body = cls(
            entity_groups=entity_groups,
        )

        return ai_entity_group_credit_limits_update_body
