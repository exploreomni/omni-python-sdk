from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.suggestion_evidence_item import SuggestionEvidenceItem
    from ..models.suggestion_proposed_changes import SuggestionProposedChanges


T = TypeVar("T", bound="ModelSuggestion")


@_attrs_define
class ModelSuggestion:
    """
    Attributes:
        ai_modified_at (datetime.datetime): ISO 8601 timestamp of the last AI write (create or AI update). Unaffected by
            dismiss/restore.
        category (str): Suggestion category, e.g. `missing_context`. Example: missing_context.
        created_at (datetime.datetime): ISO 8601 timestamp of when the suggestion was created.
        evidence (list[SuggestionEvidenceItem] | None): Source evidence for the suggestion. Null for rows created before
            evidence was tracked; `[]` when none was cited.
        id (UUID): Unique identifier for the suggestion.
        ignore_reason (None | str): Optional free-text reason recorded when the suggestion was dismissed.
        ignored_at (datetime.datetime | None): ISO 8601 timestamp of dismissal, or null if active.
        ignored_by (None | UUID): User id that dismissed the suggestion, or null if active.
        priority (int): Priority from 1 (highest) to 10 (lowest). Example: 1.
        proposed_changes (SuggestionProposedChanges): The change(s) the suggestion would apply to the model.
        rationale (str): Explanation of why the suggestion was made.
        title (str): Short human-readable title.
        updated_at (datetime.datetime): ISO 8601 timestamp of the last write of any kind, including dismiss/restore.
    """

    ai_modified_at: datetime.datetime
    category: str
    created_at: datetime.datetime
    evidence: list[SuggestionEvidenceItem] | None
    id: UUID
    ignore_reason: None | str
    ignored_at: datetime.datetime | None
    ignored_by: None | UUID
    priority: int
    proposed_changes: SuggestionProposedChanges
    rationale: str
    title: str
    updated_at: datetime.datetime
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ai_modified_at = self.ai_modified_at.isoformat()

        category = self.category

        created_at = self.created_at.isoformat()

        evidence: list[dict[str, Any]] | None
        if isinstance(self.evidence, list):
            evidence = []
            for evidence_type_0_item_data in self.evidence:
                evidence_type_0_item = evidence_type_0_item_data.to_dict()
                evidence.append(evidence_type_0_item)

        else:
            evidence = self.evidence

        id = str(self.id)

        ignore_reason: None | str
        ignore_reason = self.ignore_reason

        ignored_at: None | str
        if isinstance(self.ignored_at, datetime.datetime):
            ignored_at = self.ignored_at.isoformat()
        else:
            ignored_at = self.ignored_at

        ignored_by: None | str
        if isinstance(self.ignored_by, UUID):
            ignored_by = str(self.ignored_by)
        else:
            ignored_by = self.ignored_by

        priority = self.priority

        proposed_changes = self.proposed_changes.to_dict()

        rationale = self.rationale

        title = self.title

        updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "aiModifiedAt": ai_modified_at,
                "category": category,
                "createdAt": created_at,
                "evidence": evidence,
                "id": id,
                "ignoreReason": ignore_reason,
                "ignoredAt": ignored_at,
                "ignoredBy": ignored_by,
                "priority": priority,
                "proposedChanges": proposed_changes,
                "rationale": rationale,
                "title": title,
                "updatedAt": updated_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.suggestion_evidence_item import SuggestionEvidenceItem
        from ..models.suggestion_proposed_changes import SuggestionProposedChanges

        d = dict(src_dict)
        ai_modified_at = datetime.datetime.fromisoformat(d.pop("aiModifiedAt"))

        category = d.pop("category")

        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        def _parse_evidence(data: object) -> list[SuggestionEvidenceItem] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                evidence_type_0 = []
                _evidence_type_0 = data
                for evidence_type_0_item_data in _evidence_type_0:
                    evidence_type_0_item = SuggestionEvidenceItem.from_dict(evidence_type_0_item_data)

                    evidence_type_0.append(evidence_type_0_item)

                return evidence_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[SuggestionEvidenceItem] | None, data)

        evidence = _parse_evidence(d.pop("evidence"))

        id = UUID(d.pop("id"))

        def _parse_ignore_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        ignore_reason = _parse_ignore_reason(d.pop("ignoreReason"))

        def _parse_ignored_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ignored_at_type_0 = datetime.datetime.fromisoformat(data)

                return ignored_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        ignored_at = _parse_ignored_at(d.pop("ignoredAt"))

        def _parse_ignored_by(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                ignored_by_type_0 = UUID(data)

                return ignored_by_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        ignored_by = _parse_ignored_by(d.pop("ignoredBy"))

        priority = d.pop("priority")

        proposed_changes = SuggestionProposedChanges.from_dict(d.pop("proposedChanges"))

        rationale = d.pop("rationale")

        title = d.pop("title")

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        model_suggestion = cls(
            ai_modified_at=ai_modified_at,
            category=category,
            created_at=created_at,
            evidence=evidence,
            id=id,
            ignore_reason=ignore_reason,
            ignored_at=ignored_at,
            ignored_by=ignored_by,
            priority=priority,
            proposed_changes=proposed_changes,
            rationale=rationale,
            title=title,
            updated_at=updated_at,
        )

        model_suggestion.additional_properties = d
        return model_suggestion

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
