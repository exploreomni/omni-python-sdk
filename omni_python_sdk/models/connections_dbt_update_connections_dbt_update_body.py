from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connections_dbt_update_connections_dbt_update_body_project_root_path_type_1 import (
    ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1,
    check_connections_dbt_update_connections_dbt_update_body_project_root_path_type_1,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectionsDbtUpdateConnectionsDbtUpdateBody")


@_attrs_define
class ConnectionsDbtUpdateConnectionsDbtUpdateBody:
    """dbt repository configuration

    Attributes:
        autogen_relationships (bool): Automatically generate relationships from dbt Example: True.
        branch (str): Git branch name Example: main.
        enable_virtual_schemas (bool): Enable virtual schemas from dbt
        ssh_url (str): SSH URL for git repository Example: git@github.com:org/repo.git.
        dbt_version (None | str | Unset): dbt version to use. Supported: Auto, 1.11, 1.12 Example: 1.11.
        enable_semantic_layer (bool | Unset): Enable dbt semantic layer integration Default: False.
        project_root_path (ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1 | None | str | Unset): Path
            to dbt project root within repository Example: dbt_project.
        rotate_keys (bool | Unset): Rotate SSH deploy keys Default: False.
    """

    autogen_relationships: bool
    branch: str
    enable_virtual_schemas: bool
    ssh_url: str
    dbt_version: None | str | Unset = UNSET
    enable_semantic_layer: bool | Unset = False
    project_root_path: ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1 | None | str | Unset = UNSET
    rotate_keys: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        autogen_relationships = self.autogen_relationships

        branch = self.branch

        enable_virtual_schemas = self.enable_virtual_schemas

        ssh_url = self.ssh_url

        dbt_version: None | str | Unset
        if isinstance(self.dbt_version, Unset):
            dbt_version = UNSET
        else:
            dbt_version = self.dbt_version

        enable_semantic_layer = self.enable_semantic_layer

        project_root_path: None | str | Unset
        if isinstance(self.project_root_path, Unset):
            project_root_path = UNSET
        elif isinstance(self.project_root_path, str):
            project_root_path = self.project_root_path
        else:
            project_root_path = self.project_root_path

        rotate_keys = self.rotate_keys

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "autogenRelationships": autogen_relationships,
                "branch": branch,
                "enableVirtualSchemas": enable_virtual_schemas,
                "sshUrl": ssh_url,
            }
        )
        if dbt_version is not UNSET:
            field_dict["dbtVersion"] = dbt_version
        if enable_semantic_layer is not UNSET:
            field_dict["enableSemanticLayer"] = enable_semantic_layer
        if project_root_path is not UNSET:
            field_dict["projectRootPath"] = project_root_path
        if rotate_keys is not UNSET:
            field_dict["rotateKeys"] = rotate_keys

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        autogen_relationships = d.pop("autogenRelationships")

        branch = d.pop("branch")

        enable_virtual_schemas = d.pop("enableVirtualSchemas")

        ssh_url = d.pop("sshUrl")

        def _parse_dbt_version(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        dbt_version = _parse_dbt_version(d.pop("dbtVersion", UNSET))

        enable_semantic_layer = d.pop("enableSemanticLayer", UNSET)

        def _parse_project_root_path(
            data: object,
        ) -> ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1 | None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                project_root_path_type_1 = (
                    check_connections_dbt_update_connections_dbt_update_body_project_root_path_type_1(data)
                )

                return project_root_path_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ConnectionsDbtUpdateConnectionsDbtUpdateBodyProjectRootPathType1 | None | str | Unset, data)

        project_root_path = _parse_project_root_path(d.pop("projectRootPath", UNSET))

        rotate_keys = d.pop("rotateKeys", UNSET)

        connections_dbt_update_connections_dbt_update_body = cls(
            autogen_relationships=autogen_relationships,
            branch=branch,
            enable_virtual_schemas=enable_virtual_schemas,
            ssh_url=ssh_url,
            dbt_version=dbt_version,
            enable_semantic_layer=enable_semantic_layer,
            project_root_path=project_root_path,
            rotate_keys=rotate_keys,
        )

        connections_dbt_update_connections_dbt_update_body.additional_properties = d
        return connections_dbt_update_connections_dbt_update_body

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
