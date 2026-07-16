from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ModelsCommitResponse")


@_attrs_define
class ModelsCommitResponse:
    """
    Attributes:
        did_sync (bool): Whether a sync operation was performed against git
        git_sha (None | str): The git SHA of the commit that was pushed (null if no commit was needed)
        in_sync (bool): Whether the branch is in sync with git after the operation
        pr_url (None | str): The URL of the pull request (or PR creation page for newly-created PRs). May be null when
            the underlying git provider is not recognized.
    """

    did_sync: bool
    git_sha: None | str
    in_sync: bool
    pr_url: None | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        did_sync = self.did_sync

        git_sha: None | str
        git_sha = self.git_sha

        in_sync = self.in_sync

        pr_url: None | str
        pr_url = self.pr_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "did_sync": did_sync,
                "git_sha": git_sha,
                "in_sync": in_sync,
                "pr_url": pr_url,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        did_sync = d.pop("did_sync")

        def _parse_git_sha(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        git_sha = _parse_git_sha(d.pop("git_sha"))

        in_sync = d.pop("in_sync")

        def _parse_pr_url(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        pr_url = _parse_pr_url(d.pop("pr_url"))

        models_commit_response = cls(
            did_sync=did_sync,
            git_sha=git_sha,
            in_sync=in_sync,
            pr_url=pr_url,
        )

        models_commit_response.additional_properties = d
        return models_commit_response

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
