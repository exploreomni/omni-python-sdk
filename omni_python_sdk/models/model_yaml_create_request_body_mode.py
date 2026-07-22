from typing import Literal

ModelYamlCreateRequestBodyMode = Literal["combined", "extension", "fully-resolved", "merged", "staged"]

MODEL_YAML_CREATE_REQUEST_BODY_MODE_VALUES: set[ModelYamlCreateRequestBodyMode] = {
    "combined",
    "extension",
    "fully-resolved",
    "merged",
    "staged",
}


def check_model_yaml_create_request_body_mode(value: str) -> ModelYamlCreateRequestBodyMode:
    if value in MODEL_YAML_CREATE_REQUEST_BODY_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODEL_YAML_CREATE_REQUEST_BODY_MODE_VALUES!r}")
