from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiEntityGroupCreditLimitEntry")


@_attrs_define
class AiEntityGroupCreditLimitEntry:
    """
    Attributes:
        entity (str): The embed entity's identifier (the SSO `entity` value). Example: acme-corp.
        credit_limit (float | None | Unset): The entity group's individual AI credit limit for the billing period, or
            `null` for unlimited. Either way this overrides the org default. Mutually exclusive with `useDefaultLimit`.
            Example: 50.
        use_default_limit (bool | Unset): Removes the entity group's individual limit so it follows the org default.
            Mutually exclusive with `creditLimit`.
    """

    entity: str
    credit_limit: float | None | Unset = UNSET
    use_default_limit: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity

        credit_limit: float | None | Unset
        if isinstance(self.credit_limit, Unset):
            credit_limit = UNSET
        else:
            credit_limit = self.credit_limit

        use_default_limit = self.use_default_limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
            }
        )
        if credit_limit is not UNSET:
            field_dict["creditLimit"] = credit_limit
        if use_default_limit is not UNSET:
            field_dict["useDefaultLimit"] = use_default_limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        entity = d.pop("entity")

        def _parse_credit_limit(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        credit_limit = _parse_credit_limit(d.pop("creditLimit", UNSET))

        use_default_limit = d.pop("useDefaultLimit", UNSET)

        ai_entity_group_credit_limit_entry = cls(
            entity=entity,
            credit_limit=credit_limit,
            use_default_limit=use_default_limit,
        )

        return ai_entity_group_credit_limit_entry
