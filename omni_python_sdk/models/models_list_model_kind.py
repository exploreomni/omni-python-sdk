from typing import Literal

ModelsListModelKind = Literal["BRANCH", "QUERY", "SCHEMA", "SHARED", "SHARED_EXTENSION", "WORKBOOK"]

MODELS_LIST_MODEL_KIND_VALUES: set[ModelsListModelKind] = {
    "BRANCH",
    "QUERY",
    "SCHEMA",
    "SHARED",
    "SHARED_EXTENSION",
    "WORKBOOK",
}


def check_models_list_model_kind(value: str) -> ModelsListModelKind:
    if value in MODELS_LIST_MODEL_KIND_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_LIST_MODEL_KIND_VALUES!r}")
