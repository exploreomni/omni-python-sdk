from typing import Literal

ModelsDeleteTopicMode = Literal["COMBINED", "EXTENSION", "MERGED"]

MODELS_DELETE_TOPIC_MODE_VALUES: set[ModelsDeleteTopicMode] = {
    "COMBINED",
    "EXTENSION",
    "MERGED",
}


def check_models_delete_topic_mode(value: str) -> ModelsDeleteTopicMode:
    if value in MODELS_DELETE_TOPIC_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_DELETE_TOPIC_MODE_VALUES!r}")
