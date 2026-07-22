from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiCreditControlsResponse")


@_attrs_define
class AiCreditControlsResponse:
    """
    Attributes:
        account_credit_limit (float): Monthly AI credit limit for the whole Omni account (shared across every org under
            the same Salesforce account), not just this org. 0 when no limit is configured. Example: 2000.
        credits_used (float): This org's credit usage in the current billing period. Example: 450.
        downgrade_credits (float | None): Downgrade threshold, or `null` if the downgrade control is off. Example: 800.
        entity_group_default_credits (float | None): Default per-entity-group AI credit limit, or `null` when embed
            entity groups are unlimited by default. Example: 100.
        period_end (int): End of the current billing period as a Unix ms timestamp (UTC calendar-month boundary).
        period_start (int): Start of the current billing period as a Unix ms timestamp (UTC calendar-month boundary).
        shutoff_credits (float | None): Shutoff threshold, or `null` if the shutoff control is off. Example: 1200.
        user_default_credits (float | None): Default per-user AI credit limit, or `null` when users are unlimited by
            default. Example: 100.
    """

    account_credit_limit: float
    credits_used: float
    downgrade_credits: float | None
    entity_group_default_credits: float | None
    period_end: int
    period_start: int
    shutoff_credits: float | None
    user_default_credits: float | None
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        account_credit_limit = self.account_credit_limit

        credits_used = self.credits_used

        downgrade_credits: float | None
        downgrade_credits = self.downgrade_credits

        entity_group_default_credits: float | None
        entity_group_default_credits = self.entity_group_default_credits

        period_end = self.period_end

        period_start = self.period_start

        shutoff_credits: float | None
        shutoff_credits = self.shutoff_credits

        user_default_credits: float | None
        user_default_credits = self.user_default_credits

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "accountCreditLimit": account_credit_limit,
                "creditsUsed": credits_used,
                "downgradeCredits": downgrade_credits,
                "entityGroupDefaultCredits": entity_group_default_credits,
                "periodEnd": period_end,
                "periodStart": period_start,
                "shutoffCredits": shutoff_credits,
                "userDefaultCredits": user_default_credits,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        account_credit_limit = d.pop("accountCreditLimit")

        credits_used = d.pop("creditsUsed")

        def _parse_downgrade_credits(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        downgrade_credits = _parse_downgrade_credits(d.pop("downgradeCredits"))

        def _parse_entity_group_default_credits(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        entity_group_default_credits = _parse_entity_group_default_credits(d.pop("entityGroupDefaultCredits"))

        period_end = d.pop("periodEnd")

        period_start = d.pop("periodStart")

        def _parse_shutoff_credits(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        shutoff_credits = _parse_shutoff_credits(d.pop("shutoffCredits"))

        def _parse_user_default_credits(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        user_default_credits = _parse_user_default_credits(d.pop("userDefaultCredits"))

        ai_credit_controls_response = cls(
            account_credit_limit=account_credit_limit,
            credits_used=credits_used,
            downgrade_credits=downgrade_credits,
            entity_group_default_credits=entity_group_default_credits,
            period_end=period_end,
            period_start=period_start,
            shutoff_credits=shutoff_credits,
            user_default_credits=user_default_credits,
        )

        ai_credit_controls_response.additional_properties = d
        return ai_credit_controls_response

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
