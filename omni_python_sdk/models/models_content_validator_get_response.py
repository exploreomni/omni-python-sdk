from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.models_content_validator_get_response_branch_type_0 import (
        ModelsContentValidatorGetResponseBranchType0,
    )


T = TypeVar("T", bound="ModelsContentValidatorGetResponse")


@_attrs_define
class ModelsContentValidatorGetResponse:
    """
    Attributes:
        branch (ModelsContentValidatorGetResponseBranchType0 | None): Branch info (present if branch_id was specified)
        content (list[Any]): Documents with their validation results
        model_id (str): Model UUID
    """

    branch: ModelsContentValidatorGetResponseBranchType0 | None
    content: list[Any]
    model_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.models_content_validator_get_response_branch_type_0 import (
            ModelsContentValidatorGetResponseBranchType0,
        )

        branch: dict[str, Any] | None
        if isinstance(self.branch, ModelsContentValidatorGetResponseBranchType0):
            branch = self.branch.to_dict()
        else:
            branch = self.branch

        content = self.content

        model_id = self.model_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch": branch,
                "content": content,
                "model_id": model_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.models_content_validator_get_response_branch_type_0 import (
            ModelsContentValidatorGetResponseBranchType0,
        )

        d = dict(src_dict)

        def _parse_branch(data: object) -> ModelsContentValidatorGetResponseBranchType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                branch_type_0 = ModelsContentValidatorGetResponseBranchType0.from_dict(data)

                return branch_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ModelsContentValidatorGetResponseBranchType0 | None, data)

        branch = _parse_branch(d.pop("branch"))

        content = cast(list[Any], d.pop("content"))

        model_id = d.pop("model_id")

        models_content_validator_get_response = cls(
            branch=branch,
            content=content,
            model_id=model_id,
        )

        models_content_validator_get_response.additional_properties = d
        return models_content_validator_get_response

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
