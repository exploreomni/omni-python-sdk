from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_job_status_response_state import AiJobStatusResponseState, check_ai_job_status_response_state
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ai_job_status_response_error import AiJobStatusResponseError
    from ..models.ai_job_status_response_progress_type_0 import AiJobStatusResponseProgressType0


T = TypeVar("T", bound="AiJobStatusResponse")


@_attrs_define
class AiJobStatusResponse:
    """
    Attributes:
        branch_id (None | UUID): Branch ID used for model context, or null if querying the main shared model.
        conversation_id (UUID): The conversation this job belongs to. Use this to submit follow-up jobs in the same
            conversation thread. Example: 660e8400-e29b-41d4-a716-446655440001.
        created_at (datetime.datetime): When the job was submitted. Example: 2025-01-15T10:00:00.000Z.
        id (UUID): The unique identifier for this job. Example: 550e8400-e29b-41d4-a716-446655440000.
        model_id (None | UUID): The shared model ID used for query generation. Example:
            770e8400-e29b-41d4-a716-446655440002.
        omni_chat_url (str): URL to view this conversation in the Omni chat interface. Opens the chat session where the
            job actions and results are visible. Example: https://my-org.omni.co/chat/660e8400-e29b-41d4-a716-446655440001.
        organization_id (UUID): The organization that owns this job. Example: 880e8400-e29b-41d4-a716-446655440003.
        prompt (str): The natural language prompt that was submitted. Example: What are the top 5 products by revenue?.
        state (AiJobStatusResponseState): Current state of the job. Terminal states are COMPLETE, FAILED, and CANCELLED.
            Poll until the job reaches a terminal state. Example: QUEUED.
        topic_name (None | str): Topic name used to scope query generation, or null if the AI selected the topic
            automatically. Example: order_items.
        updated_at (datetime.datetime): When the job record was last modified. Example: 2025-01-15T10:00:05.000Z.
        user_id (UUID): The user ID who created (or is associated with) this job. Example:
            990e8400-e29b-41d4-a716-446655440004.
        cancelled_at (datetime.datetime | Unset): When the job was cancelled. Only present in CANCELLED state. Example:
            2025-01-15T10:00:12.000Z.
        cancelled_by (UUID | Unset): User ID of who cancelled the job. Only present in CANCELLED state. Example:
            990e8400-e29b-41d4-a716-446655440004.
        completed_at (datetime.datetime | Unset): When the job finished (successfully or with error). Present in
            COMPLETE and FAILED states. Example: 2025-01-15T10:01:30.000Z.
        error (AiJobStatusResponseError | Unset): Error details explaining why the job failed. Only present in FAILED
            state.
        execution_started_at (datetime.datetime | Unset): When execution began. Present once the job transitions from
            QUEUED to EXECUTING. May be absent on jobs that failed or were cancelled before execution started. Example:
            2025-01-15T10:00:05.000Z.
        progress (AiJobStatusResponseProgressType0 | None | Unset): Real-time progress information. Only present in
            EXECUTING state. Null if no progress has been reported yet. Updated in real-time as the AI works through
            iterations.
        result_summary (str | Unset): Markdown-formatted summary of the job result. Only present in COMPLETE state. For
            the full result with query details and data, use GET /api/v1/ai/jobs/{jobId}/result. Example: ### Top 5 Products
            by Revenue

            1. **Sunglasses** - $678,994
            2. **Jeans** - $475,072.
    """

    branch_id: None | UUID
    conversation_id: UUID
    created_at: datetime.datetime
    id: UUID
    model_id: None | UUID
    omni_chat_url: str
    organization_id: UUID
    prompt: str
    state: AiJobStatusResponseState
    topic_name: None | str
    updated_at: datetime.datetime
    user_id: UUID
    cancelled_at: datetime.datetime | Unset = UNSET
    cancelled_by: UUID | Unset = UNSET
    completed_at: datetime.datetime | Unset = UNSET
    error: AiJobStatusResponseError | Unset = UNSET
    execution_started_at: datetime.datetime | Unset = UNSET
    progress: AiJobStatusResponseProgressType0 | None | Unset = UNSET
    result_summary: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.ai_job_status_response_progress_type_0 import AiJobStatusResponseProgressType0

        branch_id: None | str
        if isinstance(self.branch_id, UUID):
            branch_id = str(self.branch_id)
        else:
            branch_id = self.branch_id

        conversation_id = str(self.conversation_id)

        created_at = self.created_at.isoformat()

        id = str(self.id)

        model_id: None | str
        if isinstance(self.model_id, UUID):
            model_id = str(self.model_id)
        else:
            model_id = self.model_id

        omni_chat_url = self.omni_chat_url

        organization_id = str(self.organization_id)

        prompt = self.prompt

        state: str = self.state

        topic_name: None | str
        topic_name = self.topic_name

        updated_at = self.updated_at.isoformat()

        user_id = str(self.user_id)

        cancelled_at: str | Unset = UNSET
        if not isinstance(self.cancelled_at, Unset):
            cancelled_at = self.cancelled_at.isoformat()

        cancelled_by: str | Unset = UNSET
        if not isinstance(self.cancelled_by, Unset):
            cancelled_by = str(self.cancelled_by)

        completed_at: str | Unset = UNSET
        if not isinstance(self.completed_at, Unset):
            completed_at = self.completed_at.isoformat()

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        execution_started_at: str | Unset = UNSET
        if not isinstance(self.execution_started_at, Unset):
            execution_started_at = self.execution_started_at.isoformat()

        progress: dict[str, Any] | None | Unset
        if isinstance(self.progress, Unset):
            progress = UNSET
        elif isinstance(self.progress, AiJobStatusResponseProgressType0):
            progress = self.progress.to_dict()
        else:
            progress = self.progress

        result_summary = self.result_summary

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branchId": branch_id,
                "conversationId": conversation_id,
                "createdAt": created_at,
                "id": id,
                "modelId": model_id,
                "omniChatUrl": omni_chat_url,
                "organizationId": organization_id,
                "prompt": prompt,
                "state": state,
                "topicName": topic_name,
                "updatedAt": updated_at,
                "userId": user_id,
            }
        )
        if cancelled_at is not UNSET:
            field_dict["cancelledAt"] = cancelled_at
        if cancelled_by is not UNSET:
            field_dict["cancelledBy"] = cancelled_by
        if completed_at is not UNSET:
            field_dict["completedAt"] = completed_at
        if error is not UNSET:
            field_dict["error"] = error
        if execution_started_at is not UNSET:
            field_dict["executionStartedAt"] = execution_started_at
        if progress is not UNSET:
            field_dict["progress"] = progress
        if result_summary is not UNSET:
            field_dict["resultSummary"] = result_summary

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_job_status_response_error import AiJobStatusResponseError
        from ..models.ai_job_status_response_progress_type_0 import AiJobStatusResponseProgressType0

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

        conversation_id = UUID(d.pop("conversationId"))

        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        id = UUID(d.pop("id"))

        def _parse_model_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_id_type_0 = UUID(data)

                return model_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        model_id = _parse_model_id(d.pop("modelId"))

        omni_chat_url = d.pop("omniChatUrl")

        organization_id = UUID(d.pop("organizationId"))

        prompt = d.pop("prompt")

        state = check_ai_job_status_response_state(d.pop("state"))

        def _parse_topic_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        topic_name = _parse_topic_name(d.pop("topicName"))

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        user_id = UUID(d.pop("userId"))

        _cancelled_at = d.pop("cancelledAt", UNSET)
        cancelled_at: datetime.datetime | Unset
        if isinstance(_cancelled_at, Unset):
            cancelled_at = UNSET
        else:
            cancelled_at = datetime.datetime.fromisoformat(_cancelled_at)

        _cancelled_by = d.pop("cancelledBy", UNSET)
        cancelled_by: UUID | Unset
        if isinstance(_cancelled_by, Unset):
            cancelled_by = UNSET
        else:
            cancelled_by = UUID(_cancelled_by)

        _completed_at = d.pop("completedAt", UNSET)
        completed_at: datetime.datetime | Unset
        if isinstance(_completed_at, Unset):
            completed_at = UNSET
        else:
            completed_at = datetime.datetime.fromisoformat(_completed_at)

        _error = d.pop("error", UNSET)
        error: AiJobStatusResponseError | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = AiJobStatusResponseError.from_dict(_error)

        _execution_started_at = d.pop("executionStartedAt", UNSET)
        execution_started_at: datetime.datetime | Unset
        if isinstance(_execution_started_at, Unset):
            execution_started_at = UNSET
        else:
            execution_started_at = datetime.datetime.fromisoformat(_execution_started_at)

        def _parse_progress(data: object) -> AiJobStatusResponseProgressType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                progress_type_0 = AiJobStatusResponseProgressType0.from_dict(data)

                return progress_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(AiJobStatusResponseProgressType0 | None | Unset, data)

        progress = _parse_progress(d.pop("progress", UNSET))

        result_summary = d.pop("resultSummary", UNSET)

        ai_job_status_response = cls(
            branch_id=branch_id,
            conversation_id=conversation_id,
            created_at=created_at,
            id=id,
            model_id=model_id,
            omni_chat_url=omni_chat_url,
            organization_id=organization_id,
            prompt=prompt,
            state=state,
            topic_name=topic_name,
            updated_at=updated_at,
            user_id=user_id,
            cancelled_at=cancelled_at,
            cancelled_by=cancelled_by,
            completed_at=completed_at,
            error=error,
            execution_started_at=execution_started_at,
            progress=progress,
            result_summary=result_summary,
        )

        ai_job_status_response.additional_properties = d
        return ai_job_status_response

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
