from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.models_create_models_create_response_model import ModelsCreateModelsCreateResponseModel


T = TypeVar("T", bound="ModelsCreateModelsCreateResponse")


@_attrs_define
class ModelsCreateModelsCreateResponse:
    """Create model response

    Attributes:
        success (bool): Whether the operation succeeded
        error (str | Unset): Error message if creation failed
        message (str | Unset): Additional message
        model (ModelsCreateModelsCreateResponseModel | Unset): Created model details
    """

    success: bool
    error: str | Unset = UNSET
    message: str | Unset = UNSET
    model: ModelsCreateModelsCreateResponseModel | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        success = self.success

        error = self.error

        message = self.message

        model: dict[str, Any] | Unset = UNSET
        if not isinstance(self.model, Unset):
            model = self.model.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "success": success,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if message is not UNSET:
            field_dict["message"] = message
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_create_models_create_response_model import ModelsCreateModelsCreateResponseModel

        d = dict(src_dict)
        success = d.pop("success")

        error = d.pop("error", UNSET)

        message = d.pop("message", UNSET)

        _model = d.pop("model", UNSET)
        model: ModelsCreateModelsCreateResponseModel | Unset
        if isinstance(_model, Unset):
            model = UNSET
        else:
            model = ModelsCreateModelsCreateResponseModel.from_dict(_model)

        models_create_models_create_response = cls(
            success=success,
            error=error,
            message=message,
            model=model,
        )

        models_create_models_create_response.additional_properties = d
        return models_create_models_create_response

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
