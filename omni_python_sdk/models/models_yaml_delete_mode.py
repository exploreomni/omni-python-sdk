from typing import Literal

ModelsYamlDeleteMode = Literal["combined", "extension", "fully-resolved", "merged", "staged"]

MODELS_YAML_DELETE_MODE_VALUES: set[ModelsYamlDeleteMode] = {
    "combined",
    "extension",
    "fully-resolved",
    "merged",
    "staged",
}


def check_models_yaml_delete_mode(value: str) -> ModelsYamlDeleteMode:
    if value in MODELS_YAML_DELETE_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_YAML_DELETE_MODE_VALUES!r}")
