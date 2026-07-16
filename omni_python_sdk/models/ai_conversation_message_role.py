from typing import Literal

AiConversationMessageRole = Literal["assistant", "user"]

AI_CONVERSATION_MESSAGE_ROLE_VALUES: set[AiConversationMessageRole] = {
    "assistant",
    "user",
}


def check_ai_conversation_message_role(value: str) -> AiConversationMessageRole:
    if value in AI_CONVERSATION_MESSAGE_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {AI_CONVERSATION_MESSAGE_ROLE_VALUES!r}")
