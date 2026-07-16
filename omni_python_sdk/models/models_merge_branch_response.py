from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ModelsMergeBranchResponse")


@_attrs_define
class ModelsMergeBranchResponse:
    """
    Attributes:
        failed_drafts_count (float): Number of drafts that failed to publish
        git_synced (bool): Whether git was synced
        published_drafts_count (float): Number of drafts published
        success (bool): Whether the merge succeeded
    """

    failed_drafts_count: float
    git_synced: bool
    published_drafts_count: float
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        failed_drafts_count = self.failed_drafts_count

        git_synced = self.git_synced

        published_drafts_count = self.published_drafts_count

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "failed_drafts_count": failed_drafts_count,
                "git_synced": git_synced,
                "published_drafts_count": published_drafts_count,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        failed_drafts_count = d.pop("failed_drafts_count")

        git_synced = d.pop("git_synced")

        published_drafts_count = d.pop("published_drafts_count")

        success = d.pop("success")

        models_merge_branch_response = cls(
            failed_drafts_count=failed_drafts_count,
            git_synced=git_synced,
            published_drafts_count=published_drafts_count,
            success=success,
        )

        models_merge_branch_response.additional_properties = d
        return models_merge_branch_response

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
