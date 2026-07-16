from typing import Literal

ModelsContentValidatorGetFindType = Literal["FIELD", "TOPIC", "VIEW"]

MODELS_CONTENT_VALIDATOR_GET_FIND_TYPE_VALUES: set[ModelsContentValidatorGetFindType] = {
    "FIELD",
    "TOPIC",
    "VIEW",
}


def check_models_content_validator_get_find_type(value: str) -> ModelsContentValidatorGetFindType:
    if value in MODELS_CONTENT_VALIDATOR_GET_FIND_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_CONTENT_VALIDATOR_GET_FIND_TYPE_VALUES!r}")
