from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.suggestion_proposed_changes_kind import (
    SuggestionProposedChangesKind,
    check_suggestion_proposed_changes_kind,
)

if TYPE_CHECKING:
    from ..models.suggestion_context_edit import SuggestionContextEdit


T = TypeVar("T", bound="SuggestionProposedChanges")


@_attrs_define
class SuggestionProposedChanges:
    """The change(s) the suggestion would apply to the model.

    Attributes:
        edits (list[SuggestionContextEdit]):
        kind (SuggestionProposedChangesKind):
    """

    edits: list[SuggestionContextEdit]
    kind: SuggestionProposedChangesKind
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        edits = []
        for edits_item_data in self.edits:
            edits_item = edits_item_data.to_dict()
            edits.append(edits_item)

        kind: str = self.kind

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "edits": edits,
                "kind": kind,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.suggestion_context_edit import SuggestionContextEdit

        d = dict(src_dict)
        edits = []
        _edits = d.pop("edits")
        for edits_item_data in _edits:
            edits_item = SuggestionContextEdit.from_dict(edits_item_data)

            edits.append(edits_item)

        kind = check_suggestion_proposed_changes_kind(d.pop("kind"))

        suggestion_proposed_changes = cls(
            edits=edits,
            kind=kind,
        )

        suggestion_proposed_changes.additional_properties = d
        return suggestion_proposed_changes

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
