from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.routine_email_destination import RoutineEmailDestination
    from ..models.routine_slack_destination import RoutineSlackDestination


T = TypeVar("T", bound="RoutineCreateBody")


@_attrs_define
class RoutineCreateBody:
    """
    Attributes:
        model_id (UUID): The UUID of the shared model the prompt runs against. Only shared models are supported.
            Example: 770e8400-e29b-41d4-a716-446655440002.
        name (str): Customer-visible name of the routine. Used as the email subject for email destinations, and shown on
            Slack deliveries. Example: Weekly user signups.
        prompt (str): Natural language prompt Omni runs on each scheduled run. Example: How many users signed up last
            week?.
        schedule (str): Six-field cron expression (minute, hour, day-of-month, month, day-of-week, year; use `?` for an
            unspecified day field). Minimum frequency is once per hour; contact Omni support if you need more frequent
            scheduling. Example: 0 9 ? * MON *.
        timezone (str): IANA timezone identifier used to evaluate the schedule. Example: America/New_York.
        destination (RoutineEmailDestination | RoutineSlackDestination): Single delivery destination for the routine. To
            send results to multiple destinations, create one routine per destination. Omni runs the prompt once per
            scheduled run using the routine owner's permissions, and every recipient receives the same result regardless of
            their own permissions.
        branch_id (UUID | Unset): Optional branch ID for the model. Must be a branch of the shared model specified by
            modelId. Example: 550e8400-e29b-41d4-a716-446655440000.
        description (str | Unset): Optional human-readable notes about the routine. Display-only — never used as model
            input. Example: Weekly signups summary for the growth team..
        topic_name (str | Unset): Topic name to scope query generation. If omitted, the AI picks the best topic.
            Example: users.
    """

    model_id: UUID
    name: str
    prompt: str
    schedule: str
    timezone: str
    destination: RoutineEmailDestination | RoutineSlackDestination
    branch_id: UUID | Unset = UNSET
    description: str | Unset = UNSET
    topic_name: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.routine_email_destination import RoutineEmailDestination

        model_id = str(self.model_id)

        name = self.name

        prompt = self.prompt

        schedule = self.schedule

        timezone = self.timezone

        destination: dict[str, Any]
        if isinstance(self.destination, RoutineEmailDestination):
            destination = self.destination.to_dict()
        else:
            destination = self.destination.to_dict()

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        description = self.description

        topic_name = self.topic_name

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "modelId": model_id,
                "name": name,
                "prompt": prompt,
                "schedule": schedule,
                "timezone": timezone,
                "destination": destination,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if description is not UNSET:
            field_dict["description"] = description
        if topic_name is not UNSET:
            field_dict["topicName"] = topic_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.routine_email_destination import RoutineEmailDestination
        from ..models.routine_slack_destination import RoutineSlackDestination

        d = dict(src_dict)
        model_id = UUID(d.pop("modelId"))

        name = d.pop("name")

        prompt = d.pop("prompt")

        schedule = d.pop("schedule")

        timezone = d.pop("timezone")

        def _parse_destination(data: object) -> RoutineEmailDestination | RoutineSlackDestination:
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

        destination = _parse_destination(d.pop("destination"))

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        description = d.pop("description", UNSET)

        topic_name = d.pop("topicName", UNSET)

        routine_create_body = cls(
            model_id=model_id,
            name=name,
            prompt=prompt,
            schedule=schedule,
            timezone=timezone,
            destination=destination,
            branch_id=branch_id,
            description=description,
            topic_name=topic_name,
        )

        return routine_create_body
