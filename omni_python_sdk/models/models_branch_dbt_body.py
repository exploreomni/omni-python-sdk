from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsBranchDbtBody")


@_attrs_define
class ModelsBranchDbtBody:
    """
    Attributes:
        dbt_environment_id (UUID): ID of the dbt environment to activate on this branch Example:
            123e4567-e89b-12d3-a456-426614174000.
        dbt_git_branch (str | Unset): Git branch to associate with the dbt environment Example: feature/new-metrics.
    """

    dbt_environment_id: UUID
    dbt_git_branch: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dbt_environment_id = str(self.dbt_environment_id)

        dbt_git_branch = self.dbt_git_branch

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "dbt_environment_id": dbt_environment_id,
            }
        )
        if dbt_git_branch is not UNSET:
            field_dict["dbt_git_branch"] = dbt_git_branch

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dbt_environment_id = UUID(d.pop("dbt_environment_id"))

        dbt_git_branch = d.pop("dbt_git_branch", UNSET)

        models_branch_dbt_body = cls(
            dbt_environment_id=dbt_environment_id,
            dbt_git_branch=dbt_git_branch,
        )

        models_branch_dbt_body.additional_properties = d
        return models_branch_dbt_body

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
