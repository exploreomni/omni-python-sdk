from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.routine_slack_destination_slack_recipient_type import (
    RoutineSlackDestinationSlackRecipientType,
    check_routine_slack_destination_slack_recipient_type,
)
from ..models.routine_slack_destination_type import RoutineSlackDestinationType, check_routine_slack_destination_type

T = TypeVar("T", bound="RoutineSlackDestination")


@_attrs_define
class RoutineSlackDestination:
    """
    Attributes:
        recipient_id (str): The Slack channel ID (e.g. "C01234567") or user ID (e.g. "U01234567") that receives each
            scheduled run. Exactly one recipient per Slack routine. Example: C01234567.
        slack_recipient_type (RoutineSlackDestinationSlackRecipientType): Whether `recipientId` is a Slack channel or a
            user (delivered as a direct message). Example: channel.
        type_ (RoutineSlackDestinationType): Selects Slack delivery — each scheduled run is posted to one Slack channel
            or sent as a direct message to one user. Example: slack.
    """

    recipient_id: str
    slack_recipient_type: RoutineSlackDestinationSlackRecipientType
    type_: RoutineSlackDestinationType

    def to_dict(self) -> dict[str, Any]:
        recipient_id = self.recipient_id

        slack_recipient_type: str = self.slack_recipient_type

        type_: str = self.type_

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "recipientId": recipient_id,
                "slackRecipientType": slack_recipient_type,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        recipient_id = d.pop("recipientId")

        slack_recipient_type = check_routine_slack_destination_slack_recipient_type(d.pop("slackRecipientType"))

        type_ = check_routine_slack_destination_type(d.pop("type"))

        routine_slack_destination = cls(
            recipient_id=recipient_id,
            slack_recipient_type=slack_recipient_type,
            type_=type_,
        )

        return routine_slack_destination
