from typing import Literal

CreateModelSchemaBaseModelKindType3 = Literal["BRANCH"]

CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_3_VALUES: set[CreateModelSchemaBaseModelKindType3] = {
    "BRANCH",
}


def check_create_model_schema_base_model_kind_type_3(value: str) -> CreateModelSchemaBaseModelKindType3:
    if value in CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_3_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {CREATE_MODEL_SCHEMA_BASE_MODEL_KIND_TYPE_3_VALUES!r}"
    )
