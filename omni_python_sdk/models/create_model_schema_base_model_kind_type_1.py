from typing import Literal

CreateModelSchemaBaseModelKindType1 = Literal["SHARED"]

CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_1_VALUES: set[CreateModelSchemaBaseModelKindType1] = {
    "SHARED",
}


def check_create_model_schema_base_model_kind_type_1(value: str) -> CreateModelSchemaBaseModelKindType1:
    if value in CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_1_VALUES!r}"
    )
