from typing import Literal

ModelsGetViewResponseViewsItemFieldsItemType = Literal["dimension", "filter", "measure"]

MODELS_GET_VIEW_RESPONSE_VIEWS_ITEM_FIELDS_ITEM_TYPE_VALUES: set[ModelsGetViewResponseViewsItemFieldsItemType] = {
    "dimension",
    "filter",
    "measure",
}


def check_models_get_view_response_views_item_fields_item_type(
    value: str,
) -> ModelsGetViewResponseViewsItemFieldsItemType:
    if value in MODELS_GET_VIEW_RESPONSE_VIEWS_ITEM_FIELDS_ITEM_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GET_VIEW_RESPONSE_VIEWS_ITEM_FIELDS_ITEM_TYPE_VALUES!r}"
    )
