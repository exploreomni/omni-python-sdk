from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsCommitBody")


@_attrs_define
class ModelsCommitBody:
    """
    Attributes:
        branch_id (UUID): UUID of the branch to commit. Example: 123e4567-e89b-12d3-a456-426614174001.
        commit_message (str): Commit message for the git commit. Example: Add new orders view.
        allow_branch_exists (bool | Unset): If true (default), the commit succeeds whether the git branch already exists
            or not. If false, the request fails when the git branch already exists — use this to ensure only new pull
            requests are created. Cannot be false when require_branch_exists is true. Default: True. Example: True.
        require_branch_exists (bool | Unset): If true, the request fails when the git branch does not already exist —
            use this to ensure only existing pull requests are updated. Defaults to false. Cannot be true when
            allow_branch_exists is false. Default: False.
    """

    branch_id: UUID
    commit_message: str
    allow_branch_exists: bool | Unset = True
    require_branch_exists: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id = str(self.branch_id)

        commit_message = self.commit_message

        allow_branch_exists = self.allow_branch_exists

        require_branch_exists = self.require_branch_exists

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch_id": branch_id,
                "commit_message": commit_message,
            }
        )
        if allow_branch_exists is not UNSET:
            field_dict["allow_branch_exists"] = allow_branch_exists
        if require_branch_exists is not UNSET:
            field_dict["require_branch_exists"] = require_branch_exists

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        branch_id = UUID(d.pop("branch_id"))

        commit_message = d.pop("commit_message")

        allow_branch_exists = d.pop("allow_branch_exists", UNSET)

        require_branch_exists = d.pop("require_branch_exists", UNSET)

        models_commit_body = cls(
            branch_id=branch_id,
            commit_message=commit_message,
            allow_branch_exists=allow_branch_exists,
            require_branch_exists=require_branch_exists,
        )

        models_commit_body.additional_properties = d
        return models_commit_body

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
