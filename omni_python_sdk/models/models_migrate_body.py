from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsMigrateBody")


@_attrs_define
class ModelsMigrateBody:
    """
    Attributes:
        target_model_id (UUID): Target model ID to migrate to
        branch_name (str | Unset): Branch name for the target model
        commit_message (str | Unset): Commit message for git sync
        delete_views_and_topics_missing_from_source (bool | Unset): When true (default), views and topics in the target
            model that are missing from the migrated source are deleted (the source is treated as the complete model). When
            false, they are kept (inherited) instead — useful when the source git ref may be missing objects that exist in
            omni but not in git, e.g. a newly synced schema. Default: True.
        git_ref (str | Unset): Git reference
    """

    target_model_id: UUID
    branch_name: str | Unset = UNSET
    commit_message: str | Unset = UNSET
    delete_views_and_topics_missing_from_source: bool | Unset = True
    git_ref: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        target_model_id = str(self.target_model_id)

        branch_name = self.branch_name

        commit_message = self.commit_message

        delete_views_and_topics_missing_from_source = self.delete_views_and_topics_missing_from_source

        git_ref = self.git_ref

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "targetModelId": target_model_id,
            }
        )
        if branch_name is not UNSET:
            field_dict["branchName"] = branch_name
        if commit_message is not UNSET:
            field_dict["commitMessage"] = commit_message
        if delete_views_and_topics_missing_from_source is not UNSET:
            field_dict["deleteViewsAndTopicsMissingFromSource"] = delete_views_and_topics_missing_from_source
        if git_ref is not UNSET:
            field_dict["gitRef"] = git_ref

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        target_model_id = UUID(d.pop("targetModelId"))

        branch_name = d.pop("branchName", UNSET)

        commit_message = d.pop("commitMessage", UNSET)

        delete_views_and_topics_missing_from_source = d.pop("deleteViewsAndTopicsMissingFromSource", UNSET)

        git_ref = d.pop("gitRef", UNSET)

        models_migrate_body = cls(
            target_model_id=target_model_id,
            branch_name=branch_name,
            commit_message=commit_message,
            delete_views_and_topics_missing_from_source=delete_views_and_topics_missing_from_source,
            git_ref=git_ref,
        )

        models_migrate_body.additional_properties = d
        return models_migrate_body

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
