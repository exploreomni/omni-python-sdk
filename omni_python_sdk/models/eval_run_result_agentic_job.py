from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.eval_run_result_agentic_job_state import (
    EvalRunResultAgenticJobState,
    check_eval_run_result_agentic_job_state,
)

T = TypeVar("T", bound="EvalRunResultAgenticJob")


@_attrs_define
class EvalRunResultAgenticJob:
    """
    Attributes:
        conversation_id (None | UUID): Conversation the agentic job belongs to. Example:
            770e8400-e29b-41d4-a716-446655440002.
        id (UUID): Agentic job identifier. Example: 990e8400-e29b-41d4-a716-446655440004.
        state (EvalRunResultAgenticJobState): Current state of the agentic job that ran this prompt. Example: COMPLETE.
    """

    conversation_id: None | UUID
    id: UUID
    state: EvalRunResultAgenticJobState
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        conversation_id: None | str
        if isinstance(self.conversation_id, UUID):
            conversation_id = str(self.conversation_id)
        else:
            conversation_id = self.conversation_id

        id = str(self.id)

        state: str = self.state

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conversation_id": conversation_id,
                "id": id,
                "state": state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_conversation_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                conversation_id_type_0 = UUID(data)

                return conversation_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        conversation_id = _parse_conversation_id(d.pop("conversation_id"))

        id = UUID(d.pop("id"))

        state = check_eval_run_result_agentic_job_state(d.pop("state"))

        eval_run_result_agentic_job = cls(
            conversation_id=conversation_id,
            id=id,
            state=state,
        )

        eval_run_result_agentic_job.additional_properties = d
        return eval_run_result_agentic_job

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
