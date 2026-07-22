from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.suggestion_run_status import SuggestionRunStatus, check_suggestion_run_status
from ..models.suggestion_run_trigger_source import SuggestionRunTriggerSource, check_suggestion_run_trigger_source

if TYPE_CHECKING:
    from ..models.suggestion_run_error_type_0 import SuggestionRunErrorType0
    from ..models.suggestion_run_triggered_by_type_0 import SuggestionRunTriggeredByType0


T = TypeVar("T", bound="SuggestionRun")


@_attrs_define
class SuggestionRun:
    """
    Attributes:
        completed_at (datetime.datetime | None): ISO 8601 timestamp when the run reached a terminal state.
        created_at (datetime.datetime): ISO 8601 timestamp when the run was created (queued).
        error (None | SuggestionRunErrorType0): Failure details when `status` is `failed`; null otherwise.
        execution_started_at (datetime.datetime | None): ISO 8601 timestamp when the worker started executing; null
            while queued.
        id (UUID): The run id.
        status (SuggestionRunStatus): Run status. Terminal states are `complete` and `failed`.
        trigger_source (SuggestionRunTriggerSource): Whether the run was triggered manually or by the schedule.
        triggered_by (None | SuggestionRunTriggeredByType0): The user who triggered a manual run; null for scheduled
            runs.
    """

    completed_at: datetime.datetime | None
    created_at: datetime.datetime
    error: None | SuggestionRunErrorType0
    execution_started_at: datetime.datetime | None
    id: UUID
    status: SuggestionRunStatus
    trigger_source: SuggestionRunTriggerSource
    triggered_by: None | SuggestionRunTriggeredByType0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.suggestion_run_error_type_0 import SuggestionRunErrorType0
        from ..models.suggestion_run_triggered_by_type_0 import SuggestionRunTriggeredByType0

        completed_at: None | str
        if isinstance(self.completed_at, datetime.datetime):
            completed_at = self.completed_at.isoformat()
        else:
            completed_at = self.completed_at

        created_at = self.created_at.isoformat()

        error: dict[str, Any] | None
        if isinstance(self.error, SuggestionRunErrorType0):
            error = self.error.to_dict()
        else:
            error = self.error

        execution_started_at: None | str
        if isinstance(self.execution_started_at, datetime.datetime):
            execution_started_at = self.execution_started_at.isoformat()
        else:
            execution_started_at = self.execution_started_at

        id = str(self.id)

        status: str = self.status

        trigger_source: str = self.trigger_source

        triggered_by: dict[str, Any] | None
        if isinstance(self.triggered_by, SuggestionRunTriggeredByType0):
            triggered_by = self.triggered_by.to_dict()
        else:
            triggered_by = self.triggered_by

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "completedAt": completed_at,
                "createdAt": created_at,
                "error": error,
                "executionStartedAt": execution_started_at,
                "id": id,
                "status": status,
                "triggerSource": trigger_source,
                "triggeredBy": triggered_by,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.suggestion_run_error_type_0 import SuggestionRunErrorType0
        from ..models.suggestion_run_triggered_by_type_0 import SuggestionRunTriggeredByType0

        d = dict(src_dict)

        def _parse_completed_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                completed_at_type_0 = datetime.datetime.fromisoformat(data)

                return completed_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        completed_at = _parse_completed_at(d.pop("completedAt"))

        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        def _parse_error(data: object) -> None | SuggestionRunErrorType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                error_type_0 = SuggestionRunErrorType0.from_dict(data)

                return error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | SuggestionRunErrorType0, data)

        error = _parse_error(d.pop("error"))

        def _parse_execution_started_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                execution_started_at_type_0 = datetime.datetime.fromisoformat(data)

                return execution_started_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        execution_started_at = _parse_execution_started_at(d.pop("executionStartedAt"))

        id = UUID(d.pop("id"))

        status = check_suggestion_run_status(d.pop("status"))

        trigger_source = check_suggestion_run_trigger_source(d.pop("triggerSource"))

        def _parse_triggered_by(data: object) -> None | SuggestionRunTriggeredByType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                triggered_by_type_0 = SuggestionRunTriggeredByType0.from_dict(data)

                return triggered_by_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | SuggestionRunTriggeredByType0, data)

        triggered_by = _parse_triggered_by(d.pop("triggeredBy"))

        suggestion_run = cls(
            completed_at=completed_at,
            created_at=created_at,
            error=error,
            execution_started_at=execution_started_at,
            id=id,
            status=status,
            trigger_source=trigger_source,
            triggered_by=triggered_by,
        )

        suggestion_run.additional_properties = d
        return suggestion_run

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
