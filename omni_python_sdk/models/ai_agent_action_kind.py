from typing import Literal

AiAgentActionKind = Literal["sample", "skill"]

AI_AGENT_ACTION_KIND_VALUES: set[AiAgentActionKind] = {
    "sample",
    "skill",
}


def check_ai_agent_action_kind(value: str) -> AiAgentActionKind:
    if value in AI_AGENT_ACTION_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_AGENT_ACTION_KIND_VALUES!r}")
