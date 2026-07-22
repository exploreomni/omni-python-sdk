from typing import Literal

ModelsDeleteViewMode = Literal["COMBINED", "EXTENSION", "MERGED"]

MODELS_DELETE_VIEW_MODE_VALUES: set[ModelsDeleteViewMode] = {
    "COMBINED",
    "EXTENSION",
    "MERGED",
}


def check_models_delete_view_mode(value: str) -> ModelsDeleteViewMode:
    if value in MODELS_DELETE_VIEW_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_DELETE_VIEW_MODE_VALUES!r}")
