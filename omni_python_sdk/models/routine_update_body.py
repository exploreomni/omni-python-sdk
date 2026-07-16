from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.routine_email_destination import RoutineEmailDestination
    from ..models.routine_slack_destination import RoutineSlackDestination


T = TypeVar("T", bound="RoutineUpdateBody")


@_attrs_define
class RoutineUpdateBody:
    """
    Attributes:
        description (None | str | Unset): Display-only notes about the routine. Pass null to clear it.
        destination (RoutineEmailDestination | RoutineSlackDestination | Unset): Single delivery destination for the
            routine. To send results to multiple destinations, create one routine per destination. Omni runs the prompt once
            per scheduled run using the routine owner's permissions, and every recipient receives the same result regardless
            of their own permissions.
        name (str | Unset): New customer-visible name of the routine. Used as the email subject for email destinations,
            and shown on Slack deliveries.
        prompt (str | Unset): New natural language prompt Omni runs on each scheduled run.
        schedule (str | Unset): New six-field cron expression (minute, hour, day-of-month, month, day-of-week, year; use
            `?` for an unspecified day field). Minimum frequency is once per hour.
        timezone (str | Unset): New IANA timezone identifier used to evaluate the schedule.
    """

    description: None | str | Unset = UNSET
    destination: RoutineEmailDestination | RoutineSlackDestination | Unset = UNSET
    name: str | Unset = UNSET
    prompt: str | Unset = UNSET
    schedule: str | Unset = UNSET
    timezone: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.routine_email_destination import RoutineEmailDestination

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        destination: dict[str, Any] | Unset
        if isinstance(self.destination, Unset):
            destination = UNSET
        elif isinstance(self.destination, RoutineEmailDestination):
            destination = self.destination.to_dict()
        else:
            destination = self.destination.to_dict()

        name = self.name

        prompt = self.prompt

        schedule = self.schedule

        timezone = self.timezone

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if destination is not UNSET:
            field_dict["destination"] = destination
        if name is not UNSET:
            field_dict["name"] = name
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if schedule is not UNSET:
            field_dict["schedule"] = schedule
        if timezone is not UNSET:
            field_dict["timezone"] = timezone

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.routine_email_destination import RoutineEmailDestination
        from ..models.routine_slack_destination import RoutineSlackDestination

        d = dict(src_dict)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        def _parse_destination(data: object) -> RoutineEmailDestination | RoutineSlackDestination | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_routine_destination_type_0 = RoutineEmailDestination.from_dict(data)

                return componentsschemas_routine_destination_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_routine_destination_type_1 = RoutineSlackDestination.from_dict(data)

            return componentsschemas_routine_destination_type_1

        destination = _parse_destination(d.pop("destination", UNSET))

        name = d.pop("name", UNSET)

        prompt = d.pop("prompt", UNSET)

        schedule = d.pop("schedule", UNSET)

        timezone = d.pop("timezone", UNSET)

        routine_update_body = cls(
            description=description,
            destination=destination,
            name=name,
            prompt=prompt,
            schedule=schedule,
            timezone=timezone,
        )

        return routine_update_body
