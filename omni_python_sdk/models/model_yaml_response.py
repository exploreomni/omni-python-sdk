from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.model_yaml_response_checksums import ModelYamlResponseChecksums
    from ..models.model_yaml_response_files import ModelYamlResponseFiles
    from ..models.model_yaml_response_view_names import ModelYamlResponseViewNames


T = TypeVar("T", bound="ModelYamlResponse")


@_attrs_define
class ModelYamlResponse:
    """
    Attributes:
        files (ModelYamlResponseFiles): YAML content for each file
        version (float): Model version number
        checksums (ModelYamlResponseChecksums | Unset): Checksums for each file
        view_names (ModelYamlResponseViewNames | Unset): View name mappings
    """

    files: ModelYamlResponseFiles
    version: float
    checksums: ModelYamlResponseChecksums | Unset = UNSET
    view_names: ModelYamlResponseViewNames | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        files = self.files.to_dict()

        version = self.version

        checksums: dict[str, Any] | Unset = UNSET
        if not isinstance(self.checksums, Unset):
            checksums = self.checksums.to_dict()

        view_names: dict[str, Any] | Unset = UNSET
        if not isinstance(self.view_names, Unset):
            view_names = self.view_names.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "files": files,
                "version": version,
            }
        )
        if checksums is not UNSET:
            field_dict["checksums"] = checksums
        if view_names is not UNSET:
            field_dict["viewNames"] = view_names

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.model_yaml_response_checksums import ModelYamlResponseChecksums
        from ..models.model_yaml_response_files import ModelYamlResponseFiles
        from ..models.model_yaml_response_view_names import ModelYamlResponseViewNames

        d = dict(src_dict)
        files = ModelYamlResponseFiles.from_dict(d.pop("files"))

        version = d.pop("version")

        _checksums = d.pop("checksums", UNSET)
        checksums: ModelYamlResponseChecksums | Unset
        if isinstance(_checksums, Unset):
            checksums = UNSET
        else:
            checksums = ModelYamlResponseChecksums.from_dict(_checksums)

        _view_names = d.pop("viewNames", UNSET)
        view_names: ModelYamlResponseViewNames | Unset
        if isinstance(_view_names, Unset):
            view_names = UNSET
        else:
            view_names = ModelYamlResponseViewNames.from_dict(_view_names)

        model_yaml_response = cls(
            files=files,
            version=version,
            checksums=checksums,
            view_names=view_names,
        )

        model_yaml_response.additional_properties = d
        return model_yaml_response

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
