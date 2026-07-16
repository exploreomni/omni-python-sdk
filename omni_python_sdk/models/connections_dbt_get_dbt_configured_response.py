from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsDbtGetDbtConfiguredResponse")


@_attrs_define
class ConnectionsDbtGetDbtConfiguredResponse:
    """dbt repository configuration response

    Attributes:
        autogen_relationships (bool): Whether relationships are auto-generated from dbt Example: True.
        branch (str): Git branch name Example: main.
        dbt_version (str): dbt version being used Example: Auto.
        enable_semantic_layer (bool): Whether the dbt semantic layer integration is enabled
        enable_virtual_schemas (bool): Whether virtual schemas are enabled
        project_root_path (None | str): Path to dbt project root Example: dbt_project.
        ssh_url (str): SSH URL for git repository Example: git@github.com:org/repo.git.
        supports_dbt (bool): Indicates dbt is supported and configured Example: True.
    """

    autogen_relationships: bool
    branch: str
    dbt_version: str
    enable_semantic_layer: bool
    enable_virtual_schemas: bool
    project_root_path: None | str
    ssh_url: str
    supports_dbt: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        autogen_relationships = self.autogen_relationships

        branch = self.branch

        dbt_version = self.dbt_version

        enable_semantic_layer = self.enable_semantic_layer

        enable_virtual_schemas = self.enable_virtual_schemas

        project_root_path: None | str
        project_root_path = self.project_root_path

        ssh_url = self.ssh_url

        supports_dbt = self.supports_dbt

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "autogenRelationships": autogen_relationships,
                "branch": branch,
                "dbtVersion": dbt_version,
                "enableSemanticLayer": enable_semantic_layer,
                "enableVirtualSchemas": enable_virtual_schemas,
                "projectRootPath": project_root_path,
                "sshUrl": ssh_url,
                "supportsDbt": supports_dbt,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        autogen_relationships = d.pop("autogenRelationships")

        branch = d.pop("branch")

        dbt_version = d.pop("dbtVersion")

        enable_semantic_layer = d.pop("enableSemanticLayer")

        enable_virtual_schemas = d.pop("enableVirtualSchemas")

        def _parse_project_root_path(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        project_root_path = _parse_project_root_path(d.pop("projectRootPath"))

        ssh_url = d.pop("sshUrl")

        supports_dbt = d.pop("supportsDbt")

        connections_dbt_get_dbt_configured_response = cls(
            autogen_relationships=autogen_relationships,
            branch=branch,
            dbt_version=dbt_version,
            enable_semantic_layer=enable_semantic_layer,
            enable_virtual_schemas=enable_virtual_schemas,
            project_root_path=project_root_path,
            ssh_url=ssh_url,
            supports_dbt=supports_dbt,
        )

        connections_dbt_get_dbt_configured_response.additional_properties = d
        return connections_dbt_get_dbt_configured_response

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
