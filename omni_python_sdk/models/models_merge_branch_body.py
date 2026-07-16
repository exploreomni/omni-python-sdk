from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsMergeBranchBody")


@_attrs_define
class ModelsMergeBranchBody:
    """
    Attributes:
        commit_message (str | Unset): Custom commit message for git sync
        delete_branch (bool | Unset): Delete the branch after merging Default: False.
        force_override_git_settings (bool | Unset): Override PR-required or git-follower settings Default: False.
        publish_drafts (bool | Unset): Publish branch-attached drafts Default: True.
    """

    commit_message: str | Unset = UNSET
    delete_branch: bool | Unset = False
    force_override_git_settings: bool | Unset = False
    publish_drafts: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        commit_message = self.commit_message

        delete_branch = self.delete_branch

        force_override_git_settings = self.force_override_git_settings

        publish_drafts = self.publish_drafts

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if commit_message is not UNSET:
            field_dict["commit_message"] = commit_message
        if delete_branch is not UNSET:
            field_dict["delete_branch"] = delete_branch
        if force_override_git_settings is not UNSET:
            field_dict["force_override_git_settings"] = force_override_git_settings
        if publish_drafts is not UNSET:
            field_dict["publish_drafts"] = publish_drafts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        commit_message = d.pop("commit_message", UNSET)

        delete_branch = d.pop("delete_branch", UNSET)

        force_override_git_settings = d.pop("force_override_git_settings", UNSET)

        publish_drafts = d.pop("publish_drafts", UNSET)

        models_merge_branch_body = cls(
            commit_message=commit_message,
            delete_branch=delete_branch,
            force_override_git_settings=force_override_git_settings,
            publish_drafts=publish_drafts,
        )

        models_merge_branch_body.additional_properties = d
        return models_merge_branch_body

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
