from typing import Literal

ModelsListSortField = Literal["baseModelId", "connectionId", "createdAt", "modelKind", "name", "updatedAt"]

MODELS_LIST_SORT_FIELD_VALUES: set[ModelsListSortField] = {
    "baseModelId",
    "connectionId",
    "createdAt",
    "modelKind",
    "name",
    "updatedAt",
}


def check_models_list_sort_field(value: str) -> ModelsListSortField:
    if value in MODELS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_LIST_SORT_FIELD_VALUES!r}")
