from typing import Literal

ModelsYamlGetMode = Literal["combined", "extension", "fully-resolved", "merged", "staged"]

MODELS_YAML_GET_MODE_VALUES: set[ModelsYamlGetMode] = {
    "combined",
    "extension",
    "fully-resolved",
    "merged",
    "staged",
}


def check_models_yaml_get_mode(value: str) -> ModelsYamlGetMode:
    if value in MODELS_YAML_GET_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_YAML_GET_MODE_VALUES!r}")
