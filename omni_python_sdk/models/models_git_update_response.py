from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.models_git_update_response_auth_method import (
    ModelsGitUpdateResponseAuthMethod,
    check_models_git_update_response_auth_method,
)
from ..models.models_git_update_response_require_pull_request import (
    ModelsGitUpdateResponseRequirePullRequest,
    check_models_git_update_response_require_pull_request,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsGitUpdateResponse")


@_attrs_define
class ModelsGitUpdateResponse:
    """
    Attributes:
        auth_method (ModelsGitUpdateResponseAuthMethod): Authentication method. "ssh" for deploy key, "https_token" for
            deploy token/PAT. "github_app" may appear for connections managed in Omni model settings; it cannot be created
            or modified through this API. Example: ssh.
        base_branch (str): The target branch for Omni pull requests Example: main.
        branch_per_pull_request (bool): If true, all pull requests will create a branch in Omni, even those created
            outside of the tool
        clone_url (str): Clone URL of the git repository (SSH or HTTPS) Example: git@github.com:org/repo.git.
        git_follower (bool): If true, the shared model is read-only and can only be updated by merging pull requests to
            the base branch
        git_service_provider (str): The git provider type Example: github.
        model_path (None | str): Path to model files in the repository Example: omni/my_model.
        public_key (None | str): SSH public key for repository access (deploy key). Null for HTTPS token auth. Example:
            ssh-ed25519 AAAA....
        require_pull_request (ModelsGitUpdateResponseRequirePullRequest): When pull requests are required: "always" for
            all changes, "users-only" for user-initiated changes only, "never" for direct commits. Example: users-only.
        ssh_url (str): Deprecated — use cloneUrl. Clone URL of the git repository.
        web_url (None | str): Custom web URL for the git repository, or null if not set Example:
            https://github.com/org/repo.
        webhook_url (str): Webhook URL to configure in your git provider Example:
            https://app.omni.co/api/webhooks/model/....
        webhook_secret (str | Unset): Webhook secret for signature verification. Only included if requested via
            ?include=webhookSecret
    """

    auth_method: ModelsGitUpdateResponseAuthMethod
    base_branch: str
    branch_per_pull_request: bool
    clone_url: str
    git_follower: bool
    git_service_provider: str
    model_path: None | str
    public_key: None | str
    require_pull_request: ModelsGitUpdateResponseRequirePullRequest
    ssh_url: str
    web_url: None | str
    webhook_url: str
    webhook_secret: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        auth_method: str = self.auth_method

        base_branch = self.base_branch

        branch_per_pull_request = self.branch_per_pull_request

        clone_url = self.clone_url

        git_follower = self.git_follower

        git_service_provider = self.git_service_provider

        model_path: None | str
        model_path = self.model_path

        public_key: None | str
        public_key = self.public_key

        require_pull_request: str = self.require_pull_request

        ssh_url = self.ssh_url

        web_url: None | str
        web_url = self.web_url

        webhook_url = self.webhook_url

        webhook_secret = self.webhook_secret

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "authMethod": auth_method,
                "baseBranch": base_branch,
                "branchPerPullRequest": branch_per_pull_request,
                "cloneUrl": clone_url,
                "gitFollower": git_follower,
                "gitServiceProvider": git_service_provider,
                "modelPath": model_path,
                "publicKey": public_key,
                "requirePullRequest": require_pull_request,
                "sshUrl": ssh_url,
                "webUrl": web_url,
                "webhookUrl": webhook_url,
            }
        )
        if webhook_secret is not UNSET:
            field_dict["webhookSecret"] = webhook_secret

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        auth_method = check_models_git_update_response_auth_method(d.pop("authMethod"))

        base_branch = d.pop("baseBranch")

        branch_per_pull_request = d.pop("branchPerPullRequest")

        clone_url = d.pop("cloneUrl")

        git_follower = d.pop("gitFollower")

        git_service_provider = d.pop("gitServiceProvider")

        def _parse_model_path(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        model_path = _parse_model_path(d.pop("modelPath"))

        def _parse_public_key(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        public_key = _parse_public_key(d.pop("publicKey"))

        require_pull_request = check_models_git_update_response_require_pull_request(d.pop("requirePullRequest"))

        ssh_url = d.pop("sshUrl")

        def _parse_web_url(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        web_url = _parse_web_url(d.pop("webUrl"))

        webhook_url = d.pop("webhookUrl")

        webhook_secret = d.pop("webhookSecret", UNSET)

        models_git_update_response = cls(
            auth_method=auth_method,
            base_branch=base_branch,
            branch_per_pull_request=branch_per_pull_request,
            clone_url=clone_url,
            git_follower=git_follower,
            git_service_provider=git_service_provider,
            model_path=model_path,
            public_key=public_key,
            require_pull_request=require_pull_request,
            ssh_url=ssh_url,
            web_url=web_url,
            webhook_url=webhook_url,
            webhook_secret=webhook_secret,
        )

        models_git_update_response.additional_properties = d
        return models_git_update_response

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
