from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.routine_email_destination_response import RoutineEmailDestinationResponse
    from ..models.routine_last_run_type_0 import RoutineLastRunType0
    from ..models.routine_slack_destination import RoutineSlackDestination


T = TypeVar("T", bound="RoutineResponse")


@_attrs_define
class RoutineResponse:
    """
    Attributes:
        branch_id (None | UUID): Branch of the shared model the prompt runs against, or null.
        created_at (str): ISO 8601 timestamp when the routine was created.
        description (None | str): Display-only notes about the routine, or null.
        destination (RoutineEmailDestinationResponse | RoutineSlackDestination): Delivery configuration for the routine.
        disabled (bool): Whether the owner has paused the routine.
        id (UUID): The unique identifier of the routine.
        last_run (None | RoutineLastRunType0): Most recent completed run, or null if the routine has never completed a
            run.
        model_id (UUID): The shared model the prompt runs against.
        name (str): Customer-visible name of the routine. Used as the email subject for email destinations, and shown on
            Slack deliveries.
        prompt (str): Natural language prompt Omni runs on each scheduled run.
        recipient_count (int): Number of distinct deliverable recipients. For email, user groups are expanded to members
            and duplicates removed; a Slack routine is always 1 (its single channel or DM).
        schedule (str): Six-field cron expression (minute, hour, day-of-month, month, day-of-week, year; use `?` for an
            unspecified day field).
        system_disabled (bool): Whether Omni disabled the routine because it could no longer run successfully or safely.
        system_disabled_reason (None | str): Reason Omni disabled the routine, or null.
        timezone (str): IANA timezone identifier used to evaluate the schedule.
        topic_name (None | str): Topic scoping query generation, or null.
        updated_at (str): ISO 8601 timestamp when the routine was last updated.
    """

    branch_id: None | UUID
    created_at: str
    description: None | str
    destination: RoutineEmailDestinationResponse | RoutineSlackDestination
    disabled: bool
    id: UUID
    last_run: None | RoutineLastRunType0
    model_id: UUID
    name: str
    prompt: str
    recipient_count: int
    schedule: str
    system_disabled: bool
    system_disabled_reason: None | str
    timezone: str
    topic_name: None | str
    updated_at: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.routine_email_destination_response import RoutineEmailDestinationResponse
        from ..models.routine_last_run_type_0 import RoutineLastRunType0

        branch_id: None | str
        if isinstance(self.branch_id, UUID):
            branch_id = str(self.branch_id)
        else:
            branch_id = self.branch_id

        created_at = self.created_at

        description: None | str
        description = self.description

        destination: dict[str, Any]
        if isinstance(self.destination, RoutineEmailDestinationResponse):
            destination = self.destination.to_dict()
        else:
            destination = self.destination.to_dict()

        disabled = self.disabled

        id = str(self.id)

        last_run: dict[str, Any] | None
        if isinstance(self.last_run, RoutineLastRunType0):
            last_run = self.last_run.to_dict()
        else:
            last_run = self.last_run

        model_id = str(self.model_id)

        name = self.name

        prompt = self.prompt

        recipient_count = self.recipient_count

        schedule = self.schedule

        system_disabled = self.system_disabled

        system_disabled_reason: None | str
        system_disabled_reason = self.system_disabled_reason

        timezone = self.timezone

        topic_name: None | str
        topic_name = self.topic_name

        updated_at = self.updated_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branchId": branch_id,
                "createdAt": created_at,
                "description": description,
                "destination": destination,
                "disabled": disabled,
                "id": id,
                "lastRun": last_run,
                "modelId": model_id,
                "name": name,
                "prompt": prompt,
                "recipientCount": recipient_count,
                "schedule": schedule,
                "systemDisabled": system_disabled,
                "systemDisabledReason": system_disabled_reason,
                "timezone": timezone,
                "topicName": topic_name,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.routine_email_destination_response import RoutineEmailDestinationResponse
        from ..models.routine_last_run_type_0 import RoutineLastRunType0
        from ..models.routine_slack_destination import RoutineSlackDestination

        d = dict(src_dict)

        def _parse_branch_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                branch_id_type_0 = UUID(data)

                return branch_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        branch_id = _parse_branch_id(d.pop("branchId"))

        created_at = d.pop("createdAt")

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        def _parse_destination(data: object) -> RoutineEmailDestinationResponse | RoutineSlackDestination:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_routine_destination_response_type_0 = RoutineEmailDestinationResponse.from_dict(data)

                return componentsschemas_routine_destination_response_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_routine_destination_response_type_1 = RoutineSlackDestination.from_dict(data)

            return componentsschemas_routine_destination_response_type_1

        destination = _parse_destination(d.pop("destination"))

        disabled = d.pop("disabled")

        id = UUID(d.pop("id"))

        def _parse_last_run(data: object) -> None | RoutineLastRunType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_routine_last_run_type_0 = RoutineLastRunType0.from_dict(data)

                return componentsschemas_routine_last_run_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RoutineLastRunType0, data)

        last_run = _parse_last_run(d.pop("lastRun"))

        model_id = UUID(d.pop("modelId"))

        name = d.pop("name")

        prompt = d.pop("prompt")

        recipient_count = d.pop("recipientCount")

        schedule = d.pop("schedule")

        system_disabled = d.pop("systemDisabled")

        def _parse_system_disabled_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        system_disabled_reason = _parse_system_disabled_reason(d.pop("systemDisabledReason"))

        timezone = d.pop("timezone")

        def _parse_topic_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        topic_name = _parse_topic_name(d.pop("topicName"))

        updated_at = d.pop("updatedAt")

        routine_response = cls(
            branch_id=branch_id,
            created_at=created_at,
            description=description,
            destination=destination,
            disabled=disabled,
            id=id,
            last_run=last_run,
            model_id=model_id,
            name=name,
            prompt=prompt,
            recipient_count=recipient_count,
            schedule=schedule,
            system_disabled=system_disabled,
            system_disabled_reason=system_disabled_reason,
            timezone=timezone,
            topic_name=topic_name,
            updated_at=updated_at,
        )

        routine_response.additional_properties = d
        return routine_response

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
