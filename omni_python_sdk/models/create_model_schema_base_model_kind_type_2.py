from typing import Literal

CreateModelSchemaBaseModelKindType2 = Literal["SHARED_EXTENSION"]

CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_2_VALUES: set[CreateModelSchemaBaseModelKindType2] = {
    "SHARED_EXTENSION",
}


def check_create_model_schema_base_model_kind_type_2(value: str) -> CreateModelSchemaBaseModelKindType2:
    if value in CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_2_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_2_VALUES!r}"
    )
