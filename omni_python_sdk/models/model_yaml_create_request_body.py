from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define

from ..models.model_yaml_create_request_body_mode import (
    ModelYamlCreateRequestBodyMode,
    check_model_yaml_create_request_body_mode,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelYamlCreateRequestBody")


@_attrs_define
class ModelYamlCreateRequestBody:
    """
    Attributes:
        file_name (str): File name to create or update
        yaml (str): YAML content for the file
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        mode (ModelYamlCreateRequestBodyMode | Unset): IDE mode for YAML operations Default: 'combined'.
        commit_message (str | Unset): Commit message for git sync
        fetched_at_millis (float | Unset): Timestamp when the file was fetched
        fully_resolved (bool | Unset): Treat the posted YAML as fully resolved (with the extends chain expanded). Only
            valid with mode=combined. Default: False.
        previous_checksum (str | Unset): Previous checksum for conflict detection
    """

    file_name: str
    yaml: str
    branch_id: UUID | Unset = UNSET
    mode: ModelYamlCreateRequestBodyMode | Unset = "combined"
    commit_message: str | Unset = UNSET
    fetched_at_millis: float | Unset = UNSET
    fully_resolved: bool | Unset = False
    previous_checksum: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        file_name = self.file_name

        yaml = self.yaml

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode

        commit_message = self.commit_message

        fetched_at_millis = self.fetched_at_millis

        fully_resolved = self.fully_resolved

        previous_checksum = self.previous_checksum

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fileName": file_name,
                "yaml": yaml,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if mode is not UNSET:
            field_dict["mode"] = mode
        if commit_message is not UNSET:
            field_dict["commitMessage"] = commit_message
        if fetched_at_millis is not UNSET:
            field_dict["fetchedAtMillis"] = fetched_at_millis
        if fully_resolved is not UNSET:
            field_dict["fullyResolved"] = fully_resolved
        if previous_checksum is not UNSET:
            field_dict["previousChecksum"] = previous_checksum

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        file_name = d.pop("fileName")

        yaml = d.pop("yaml")

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        _mode = d.pop("mode", UNSET)
        mode: ModelYamlCreateRequestBodyMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = check_model_yaml_create_request_body_mode(_mode)

        commit_message = d.pop("commitMessage", UNSET)

        fetched_at_millis = d.pop("fetchedAtMillis", UNSET)

        fully_resolved = d.pop("fullyResolved", UNSET)

        previous_checksum = d.pop("previousChecksum", UNSET)

        model_yaml_create_request_body = cls(
            file_name=file_name,
            yaml=yaml,
            branch_id=branch_id,
            mode=mode,
            commit_message=commit_message,
            fetched_at_millis=fetched_at_millis,
            fully_resolved=fully_resolved,
            previous_checksum=previous_checksum,
        )

        return model_yaml_create_request_body
