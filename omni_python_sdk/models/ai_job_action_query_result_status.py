from typing import Literal

AiJobActionQueryResultStatus = Literal["error", "success"]

AI_JOB_ACTION_QUERY_RESULT_STATUS_VALUES: set[AiJobActionQueryResultStatus] = {
    "error",
    "success",
}


def check_ai_job_action_query_result_status(value: str) -> AiJobActionQueryResultStatus:
    if value in AI_JOB_ACTION_QUERY_RESULT_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_JOB_ACTION_QUERY_RESULT_STATUS_VALUES!r}")
