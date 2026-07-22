from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiCreditControlsEntityGroupsListResponseRecordsItem")


@_attrs_define
class AiCreditControlsEntityGroupsListResponseRecordsItem:
    """
    Attributes:
        credit_limit (float | None): The entity group's individual AI credit limit, or `null` for an explicit unlimited
            override. Example: 50.
        entity (str): The embed entity's identifier (the SSO `entity` value). Example: acme-corp.
    """

    credit_limit: float | None
    entity: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        credit_limit: float | None
        credit_limit = self.credit_limit

        entity = self.entity

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "creditLimit": credit_limit,
                "entity": entity,
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

        ai_credit_controls_entity_groups_list_response_records_item = cls(
            credit_limit=credit_limit,
            entity=entity,
        )

        ai_credit_controls_entity_groups_list_response_records_item.additional_properties = d
        return ai_credit_controls_entity_groups_list_response_records_item

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
