from typing import Literal

SuggestionProposedChangesKind = Literal["context_edits"]

SUGGESTION_PROPOSED_CHANGES_KIND_VALUES: set[SuggestionProposedChangesKind] = {
    "context_edits",
}


def check_suggestion_proposed_changes_kind(value: str) -> SuggestionProposedChangesKind:
    if value in SUGGESTION_PROPOSED_CHANGES_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SUGGESTION_PROPOSED_CHANGES_KIND_VALUES!r}")
