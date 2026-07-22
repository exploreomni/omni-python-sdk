from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="AiCreditControlsUpdateBody")


@_attrs_define
class AiCreditControlsUpdateBody:
    """
    Attributes:
        downgrade_credits (float | None | Unset): Credit usage at which AI downgrades to a cheaper model. Omit to leave
            unchanged, `null` to turn off, or a non-negative number to set. Must be at or below shutoffCredits. Example:
            800.
        entity_group_default_credits (float | None | Unset): Default per-entity-group AI credit limit for the billing
            period — what every embed entity group without an individual limit gets. Omit to leave unchanged, `null` for
            unlimited by default, or a non-negative number to set. Example: 100.
        shutoff_credits (float | None | Unset): Credit usage at which AI shuts off entirely. Omit to leave unchanged,
            `null` to turn off, or a non-negative number to set. Example: 1200.
        user_default_credits (float | None | Unset): Default per-user AI credit limit for the billing period — what
            every user without an individual limit gets. Omit to leave unchanged, `null` for unlimited by default, or a non-
            negative number to set. Example: 100.
    """

    downgrade_credits: float | None | Unset = UNSET
    entity_group_default_credits: float | None | Unset = UNSET
    shutoff_credits: float | None | Unset = UNSET
    user_default_credits: float | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        downgrade_credits: float | None | Unset
        if isinstance(self.downgrade_credits, Unset):
            downgrade_credits = UNSET
        else:
            downgrade_credits = self.downgrade_credits

        entity_group_default_credits: float | None | Unset
        if isinstance(self.entity_group_default_credits, Unset):
            entity_group_default_credits = UNSET
        else:
            entity_group_default_credits = self.entity_group_default_credits

        shutoff_credits: float | None | Unset
        if isinstance(self.shutoff_credits, Unset):
            shutoff_credits = UNSET
        else:
            shutoff_credits = self.shutoff_credits

        user_default_credits: float | None | Unset
        if isinstance(self.user_default_credits, Unset):
            user_default_credits = UNSET
        else:
            user_default_credits = self.user_default_credits

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if downgrade_credits is not UNSET:
            field_dict["downgradeCredits"] = downgrade_credits
        if entity_group_default_credits is not UNSET:
            field_dict["entityGroupDefaultCredits"] = entity_group_default_credits
        if shutoff_credits is not UNSET:
            field_dict["shutoffCredits"] = shutoff_credits
        if user_default_credits is not UNSET:
            field_dict["userDefaultCredits"] = user_default_credits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_downgrade_credits(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        downgrade_credits = _parse_downgrade_credits(d.pop("downgradeCredits", UNSET))

        def _parse_entity_group_default_credits(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        entity_group_default_credits = _parse_entity_group_default_credits(d.pop("entityGroupDefaultCredits", UNSET))

        def _parse_shutoff_credits(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        shutoff_credits = _parse_shutoff_credits(d.pop("shutoffCredits", UNSET))

        def _parse_user_default_credits(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        user_default_credits = _parse_user_default_credits(d.pop("userDefaultCredits", UNSET))

        ai_credit_controls_update_body = cls(
            downgrade_credits=downgrade_credits,
            entity_group_default_credits=entity_group_default_credits,
            shutoff_credits=shutoff_credits,
            user_default_credits=user_default_credits,
        )

        return ai_credit_controls_update_body
