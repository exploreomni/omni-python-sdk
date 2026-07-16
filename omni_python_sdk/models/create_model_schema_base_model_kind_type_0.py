from typing import Literal

CreateModelSchemaBaseModelKindType0 = Literal["SCHEMA"]

CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_0_VALUES: set[CreateModelSchemaBaseModelKindType0] = {
    "SCHEMA",
}


def check_create_model_schema_base_model_kind_type_0(value: str) -> CreateModelSchemaBaseModelKindType0:
    if value in CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_0_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_0_VALUES!r}"
    )
