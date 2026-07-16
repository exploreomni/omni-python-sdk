from typing import Literal

ModelsContentValidatorReplaceBodyFindOrReplaceType = Literal["FIELD", "TOPIC", "VIEW"]

MODELS_CONTENT_VALIDATOR_REPLACE_BODY_FIND_OR_REPLACE_TYPE_VALUES: set[
    ModelsContentValidatorReplaceBodyFindOrReplaceType
] = {
    "FIELD",
    "TOPIC",
    "VIEW",
}


def check_models_content_validator_replace_body_find_or_replace_type(
    value: str,
) -> ModelsContentValidatorReplaceBodyFindOrReplaceType:
    if value in MODELS_CONTENT_VALIDATOR_REPLACE_BODY_FIND_OR_REPLACE_TYPE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_CONTENT_VALIDATOR_REPLACE_BODY_FIND_OR_REPLACE_TYPE_VALUES!r}"
    )
