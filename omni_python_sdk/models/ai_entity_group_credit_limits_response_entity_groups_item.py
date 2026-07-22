from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiEntityGroupCreditLimitsResponseEntityGroupsItem")


@_attrs_define
class AiEntityGroupCreditLimitsResponseEntityGroupsItem:
    """
    Attributes:
        credit_limit (float | None): The entity group's effective AI credit limit, or `null` for unlimited. Example: 50.
        entity (str): The embed entity's identifier (the SSO `entity` value). Example: acme-corp.
        uses_default_limit (bool): True when the entity group has no individual limit and follows the org default.
    """

    credit_limit: float | None
    entity: str
    uses_default_limit: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        credit_limit: float | None
        credit_limit = self.credit_limit

        entity = self.entity

        uses_default_limit = self.uses_default_limit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "creditLimit": credit_limit,
                "entity": entity,
                "usesDefaultLimit": uses_default_limit,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_credit_limit(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        credit_limit = _parse_credit_limit(d.pop("creditLimit"))

        entity = d.pop("entity")

        uses_default_limit = d.pop("usesDefaultLimit")

        ai_entity_group_credit_limits_response_entity_groups_item = cls(
            credit_limit=credit_limit,
            entity=entity,
            uses_default_limit=uses_default_limit,
        )

        ai_entity_group_credit_limits_response_entity_groups_item.additional_properties = d
        return ai_entity_group_credit_limits_response_entity_groups_item

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
