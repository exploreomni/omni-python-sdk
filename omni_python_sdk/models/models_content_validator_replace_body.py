from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.models_content_validator_replace_body_find_or_replace_type import (
    ModelsContentValidatorReplaceBodyFindOrReplaceType,
    check_models_content_validator_replace_body_find_or_replace_type,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsContentValidatorReplaceBody")


@_attrs_define
class ModelsContentValidatorReplaceBody:
    """
    Attributes:
        find (str): The string to find
        find_or_replace_type (ModelsContentValidatorReplaceBodyFindOrReplaceType): Type of find/replace operation.
        replacement (str): The replacement string
        branch_id (str | Unset): Optional branch ID
        creator_id (UUID | Unset): Restrict replacement to documents created by this user (user ID). Unknown IDs return
            400.
        folder_paths (list[str] | Unset): Restrict replacement to documents in matching folder paths (prefix match).
            Documents with no folder are excluded unless "" is specified.
        include_personal_folders (bool | Unset): Whether to include personal folders Default: False.
        labels (str | Unset): Comma-separated label names to scope replacement. Unknown labels return 400.
        only_in_workbook_id (str | Unset): Optional workbook ID to limit the replace scope
    """

    find: str
    find_or_replace_type: ModelsContentValidatorReplaceBodyFindOrReplaceType
    replacement: str
    branch_id: str | Unset = UNSET
    creator_id: UUID | Unset = UNSET
    folder_paths: list[str] | Unset = UNSET
    include_personal_folders: bool | Unset = False
    labels: str | Unset = UNSET
    only_in_workbook_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        find = self.find

        find_or_replace_type: str = self.find_or_replace_type

        replacement = self.replacement

        branch_id = self.branch_id

        creator_id: str | Unset = UNSET
        if not isinstance(self.creator_id, Unset):
            creator_id = str(self.creator_id)

        folder_paths: list[str] | Unset = UNSET
        if not isinstance(self.folder_paths, Unset):
            folder_paths = self.folder_paths

        include_personal_folders = self.include_personal_folders

        labels = self.labels

        only_in_workbook_id = self.only_in_workbook_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "find": find,
                "find_or_replace_type": find_or_replace_type,
                "replacement": replacement,
            }
        )
        if branch_id is not UNSET:
            field_dict["branch_id"] = branch_id
        if creator_id is not UNSET:
            field_dict["creator_id"] = creator_id
        if folder_paths is not UNSET:
            field_dict["folder_paths"] = folder_paths
        if include_personal_folders is not UNSET:
            field_dict["include_personal_folders"] = include_personal_folders
        if labels is not UNSET:
            field_dict["labels"] = labels
        if only_in_workbook_id is not UNSET:
            field_dict["only_in_workbook_id"] = only_in_workbook_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        find = d.pop("find")

        find_or_replace_type = check_models_content_validator_replace_body_find_or_replace_type(
            d.pop("find_or_replace_type")
        )

        replacement = d.pop("replacement")

        branch_id = d.pop("branch_id", UNSET)

        _creator_id = d.pop("creator_id", UNSET)
        creator_id: UUID | Unset
        if isinstance(_creator_id, Unset):
            creator_id = UNSET
        else:
            creator_id = UUID(_creator_id)

        folder_paths = cast(list[str], d.pop("folder_paths", UNSET))

        include_personal_folders = d.pop("include_personal_folders", UNSET)

        labels = d.pop("labels", UNSET)

        only_in_workbook_id = d.pop("only_in_workbook_id", UNSET)

        models_content_validator_replace_body = cls(
            find=find,
            find_or_replace_type=find_or_replace_type,
            replacement=replacement,
            branch_id=branch_id,
            creator_id=creator_id,
            folder_paths=folder_paths,
            include_personal_folders=include_personal_folders,
            labels=labels,
            only_in_workbook_id=only_in_workbook_id,
        )

        models_content_validator_replace_body.additional_properties = d
        return models_content_validator_replace_body

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
