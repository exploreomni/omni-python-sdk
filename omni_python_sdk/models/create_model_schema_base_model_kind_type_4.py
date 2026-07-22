from typing import Literal

CreateModelSchemaBaseModelKindType4 = Literal["QUERY"]

CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_4_VALUES: set[CreateModelSchemaBaseModelKindType4] = {
    "QUERY",
}


def check_create_model_schema_base_model_kind_type_4(value: str) -> CreateModelSchemaBaseModelKindType4:
    if value in CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_4_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_4_VALUES!r}"
    )
