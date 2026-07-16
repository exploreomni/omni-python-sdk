from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.models_update_response_model import ModelsUpdateResponseModel


T = TypeVar("T", bound="ModelsUpdateResponse")


@_attrs_define
class ModelsUpdateResponse:
    """
    Attributes:
        model (ModelsUpdateResponseModel): Updated model details
        success (bool): Whether the operation succeeded
    """

    model: ModelsUpdateResponseModel
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model.to_dict()

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_update_response_model import ModelsUpdateResponseModel

        d = dict(src_dict)
        model = ModelsUpdateResponseModel.from_dict(d.pop("model"))

        success = d.pop("success")

        models_update_response = cls(
            model=model,
            success=success,
        )

        models_update_response.additional_properties = d
        return models_update_response

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
