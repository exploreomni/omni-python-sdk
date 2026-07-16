from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ModelsGitSyncResponse")


@_attrs_define
class ModelsGitSyncResponse:
    """
    Attributes:
        did_sync (bool): Whether a sync operation was performed
        git_sha (None | str): The git SHA after the sync operation
        in_sync (bool): Whether the model is currently in sync with git
        message (str): Human-readable message about the sync status
    """

    did_sync: bool
    git_sha: None | str
    in_sync: bool
    message: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        did_sync = self.did_sync

        git_sha: None | str
        git_sha = self.git_sha

        in_sync = self.in_sync

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "didSync": did_sync,
                "gitSha": git_sha,
                "inSync": in_sync,
                "message": message,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        did_sync = d.pop("didSync")

        def _parse_git_sha(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        git_sha = _parse_git_sha(d.pop("gitSha"))

        in_sync = d.pop("inSync")

        message = d.pop("message")

        models_git_sync_response = cls(
            did_sync=did_sync,
            git_sha=git_sha,
            in_sync=in_sync,
            message=message,
        )

        models_git_sync_response.additional_properties = d
        return models_git_sync_response

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
