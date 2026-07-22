from typing import Literal

SuggestionEvidenceItemType = Literal["ai_chat"]

SUGGESTION_EVIDENCE_ITEM_TYPE_VALUES: set[SuggestionEvidenceItemType] = {
    "ai_chat",
}


def check_suggestion_evidence_item_type(value: str) -> SuggestionEvidenceItemType:
    if value in SUGGESTION_EVIDENCE_ITEM_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SUGGESTION_EVIDENCE_ITEM_TYPE_VALUES!r}")
