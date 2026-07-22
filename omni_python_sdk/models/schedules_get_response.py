from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.schedules_get_destination import SchedulesGetDestination
    from ..models.schedules_get_response_owner import SchedulesGetResponseOwner


T = TypeVar("T", bound="SchedulesGetResponse")


@_attrs_define
class SchedulesGetResponse:
    """
    Attributes:
        condition_query_map_key (None | str): Query key used for alert condition (null for standard schedules)
        condition_type (None | str): Alert condition type: RESULTS_CHANGED, RESULTS_PRESENT, RESULTS_MISSING
        created_at (datetime.datetime): Creation timestamp
        destinations (list[SchedulesGetDestination]): Delivery destination configurations
        disabled_at (datetime.datetime | None): Timestamp when the schedule was paused (null if active)
        entity_id (str): ID of the associated dashboard
        fan_out (bool): Whether personalized fan-out delivery is enabled
        id (UUID): Schedule UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        kill_jobs_on_failure (bool): Whether to stop the job if any queries fail
        name (str): Schedule name Example: Weekly Sales Report.
        organization_id (UUID): Organization UUID
        owner (SchedulesGetResponseOwner):
        owner_id (UUID): User ID of the schedule owner
        schedule (str): AWS EventBridge cron expression (minute hour day-of-month month day-of-week year) Example: 0 9 ?
            * MON *.
        system_disabled_at (datetime.datetime | None): Timestamp when the system disabled the schedule
        system_disabled_reason (None | str): Reason for system disabling: missingQuery, noAccess,
            orphanedFilterConfigKeys
        timezone (str): IANA timezone for the schedule Example: America/New_York.
        updated_at (datetime.datetime): Last update timestamp
        filter_config (Any | Unset): The effective dashboard filter configuration that the schedule will run with: the
            dashboard's current default filters merged under the schedule's persisted overrides, with any keys no longer
            present on the dashboard dropped. This matches what is shown when the schedule is opened in the Edit Delivery
            panel, and may differ from the schedule's persisted filter configuration.
        metadata (Any | Unset): Schedule metadata including format options and delivery settings. Includes
            `timezoneOverride` (IANA timezone applied to query execution at render time, or null when no override is set).
    """

    condition_query_map_key: None | str
    condition_type: None | str
    created_at: datetime.datetime
    destinations: list[SchedulesGetDestination]
    disabled_at: datetime.datetime | None
    entity_id: str
    fan_out: bool
    id: UUID
    kill_jobs_on_failure: bool
    name: str
    organization_id: UUID
    owner: SchedulesGetResponseOwner
    owner_id: UUID
    schedule: str
    system_disabled_at: datetime.datetime | None
    system_disabled_reason: None | str
    timezone: str
    updated_at: datetime.datetime
    filter_config: Any | Unset = UNSET
    metadata: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        condition_query_map_key: None | str
        condition_query_map_key = self.condition_query_map_key

        condition_type: None | str
        condition_type = self.condition_type

        created_at = self.created_at.isoformat()

        destinations = []
        for destinations_item_data in self.destinations:
            destinations_item = destinations_item_data.to_dict()
            destinations.append(destinations_item)

        disabled_at: None | str
        if isinstance(self.disabled_at, datetime.datetime):
            disabled_at = self.disabled_at.isoformat()
        else:
            disabled_at = self.disabled_at

        entity_id = self.entity_id

        fan_out = self.fan_out

        id = str(self.id)

        kill_jobs_on_failure = self.kill_jobs_on_failure

        name = self.name

        organization_id = str(self.organization_id)

        owner = self.owner.to_dict()

        owner_id = str(self.owner_id)

        schedule = self.schedule

        system_disabled_at: None | str
        if isinstance(self.system_disabled_at, datetime.datetime):
            system_disabled_at = self.system_disabled_at.isoformat()
        else:
            system_disabled_at = self.system_disabled_at

        system_disabled_reason: None | str
        system_disabled_reason = self.system_disabled_reason

        timezone = self.timezone

        updated_at = self.updated_at.isoformat()

        filter_config = self.filter_config

        metadata = self.metadata

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conditionQueryMapKey": condition_query_map_key,
                "conditionType": condition_type,
                "createdAt": created_at,
                "destinations": destinations,
                "disabledAt": disabled_at,
                "entityId": entity_id,
                "fanOut": fan_out,
                "id": id,
                "killJobsOnFailure": kill_jobs_on_failure,
                "name": name,
                "organizationId": organization_id,
                "owner": owner,
                "ownerId": owner_id,
                "schedule": schedule,
                "systemDisabledAt": system_disabled_at,
                "systemDisabledReason": system_disabled_reason,
                "timezone": timezone,
                "updatedAt": updated_at,
            }
        )
        if filter_config is not UNSET:
            field_dict["filterConfig"] = filter_config
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.schedules_get_destination import SchedulesGetDestination
        from ..models.schedules_get_response_owner import SchedulesGetResponseOwner

        d = dict(src_dict)

        def _parse_condition_query_map_key(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        condition_query_map_key = _parse_condition_query_map_key(d.pop("conditionQueryMapKey"))

        def _parse_condition_type(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        condition_type = _parse_condition_type(d.pop("conditionType"))

        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        destinations = []
        _destinations = d.pop("destinations")
        for destinations_item_data in _destinations:
            destinations_item = SchedulesGetDestination.from_dict(destinations_item_data)

            destinations.append(destinations_item)

        def _parse_disabled_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                disabled_at_type_0 = datetime.datetime.fromisoformat(data)

                return disabled_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        disabled_at = _parse_disabled_at(d.pop("disabledAt"))

        entity_id = d.pop("entityId")

        fan_out = d.pop("fanOut")

        id = UUID(d.pop("id"))

        kill_jobs_on_failure = d.pop("killJobsOnFailure")

        name = d.pop("name")

        organization_id = UUID(d.pop("organizationId"))

        owner = SchedulesGetResponseOwner.from_dict(d.pop("owner"))

        owner_id = UUID(d.pop("ownerId"))

        schedule = d.pop("schedule")

        def _parse_system_disabled_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                system_disabled_at_type_0 = datetime.datetime.fromisoformat(data)

                return system_disabled_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        system_disabled_at = _parse_system_disabled_at(d.pop("systemDisabledAt"))

        def _parse_system_disabled_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        system_disabled_reason = _parse_system_disabled_reason(d.pop("systemDisabledReason"))

        timezone = d.pop("timezone")

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        filter_config = d.pop("filterConfig", UNSET)

        metadata = d.pop("metadata", UNSET)

        schedules_get_response = cls(
            condition_query_map_key=condition_query_map_key,
            condition_type=condition_type,
            created_at=created_at,
            destinations=destinations,
            disabled_at=disabled_at,
            entity_id=entity_id,
            fan_out=fan_out,
            id=id,
            kill_jobs_on_failure=kill_jobs_on_failure,
            name=name,
            organization_id=organization_id,
            owner=owner,
            owner_id=owner_id,
            schedule=schedule,
            system_disabled_at=system_disabled_at,
            system_disabled_reason=system_disabled_reason,
            timezone=timezone,
            updated_at=updated_at,
            filter_config=filter_config,
            metadata=metadata,
        )

        schedules_get_response.additional_properties = d
        return schedules_get_response

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
