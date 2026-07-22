from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.whoami_response_key_scope import WhoamiResponseKeyScope, check_whoami_response_key_scope
from ..models.whoami_response_org_role import WhoamiResponseOrgRole, check_whoami_response_org_role
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.whoami_response_roles_by_model import WhoamiResponseRolesByModel
    from ..models.whoami_user import WhoamiUser


T = TypeVar("T", bound="WhoamiResponse")


@_attrs_define
class WhoamiResponse:
    """
    Attributes:
        key_scope (WhoamiResponseKeyScope): Scope of the API key in use. A separate axis from role: a user-scoped key
            (PAT/OAuth) acts as a single user and cannot use SCIM, regardless of the user's org role.
        org_role (WhoamiResponseOrgRole): The caller's organization role. Example: MEMBER.
        roles_by_model (WhoamiResponseRolesByModel): Resolved role and effective permissions per model, keyed by model
            id. Connection role resolves per shared model, so this is per-model rather than a single global role.
        user (WhoamiUser):
        roles_by_model_truncated (bool | Unset): Present and `true` when `rolesByModel` was truncated because the caller
            can access more models than the unfiltered limit. Pass a `modelId` filter to retrieve specific models.
    """

    key_scope: WhoamiResponseKeyScope
    org_role: WhoamiResponseOrgRole
    roles_by_model: WhoamiResponseRolesByModel
    user: WhoamiUser
    roles_by_model_truncated: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key_scope: str = self.key_scope

        org_role: str = self.org_role

        roles_by_model = self.roles_by_model.to_dict()

        user = self.user.to_dict()

        roles_by_model_truncated = self.roles_by_model_truncated

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "keyScope": key_scope,
                "orgRole": org_role,
                "rolesByModel": roles_by_model,
                "user": user,
            }
        )
        if roles_by_model_truncated is not UNSET:
            field_dict["rolesByModelTruncated"] = roles_by_model_truncated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.whoami_response_roles_by_model import WhoamiResponseRolesByModel
        from ..models.whoami_user import WhoamiUser

        d = dict(src_dict)
        key_scope = check_whoami_response_key_scope(d.pop("keyScope"))

        org_role = check_whoami_response_org_role(d.pop("orgRole"))

        roles_by_model = WhoamiResponseRolesByModel.from_dict(d.pop("rolesByModel"))

        user = WhoamiUser.from_dict(d.pop("user"))

        roles_by_model_truncated = d.pop("rolesByModelTruncated", UNSET)

        whoami_response = cls(
            key_scope=key_scope,
            org_role=org_role,
            roles_by_model=roles_by_model,
            user=user,
            roles_by_model_truncated=roles_by_model_truncated,
        )

        whoami_response.additional_properties = d
        return whoami_response

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
