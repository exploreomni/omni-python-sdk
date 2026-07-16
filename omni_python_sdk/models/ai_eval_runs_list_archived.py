from typing import Literal

AiEvalRunsListArchived = Literal["false", "true"]

AI_EVAL_RUNS_LIST_ARCHIVED_VALUES: set[AiEvalRunsListArchived] = {
    "false",
    "true",
}


def check_ai_eval_runs_list_archived(value: str) -> AiEvalRunsListArchived:
    if value in AI_EVAL_RUNS_LIST_ARCHIVED_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_EVAL_RUNS_LIST_ARCHIVED_VALUES!r}")
