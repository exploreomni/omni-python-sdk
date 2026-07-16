from typing import Literal

AiEvalPromptSetsListArchived = Literal["false", "true"]

AI_EVAL_PROMPT_SETS_LIST_ARCHIVED_VALUES: set[AiEvalPromptSetsListArchived] = {
    "false",
    "true",
}


def check_ai_eval_prompt_sets_list_archived(value: str) -> AiEvalPromptSetsListArchived:
    if value in AI_EVAL_PROMPT_SETS_LIST_ARCHIVED_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_EVAL_PROMPT_SETS_LIST_ARCHIVED_VALUES!r}")
