from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.models_git_update_body_auth_method import (
    ModelsGitUpdateBodyAuthMethod,
    check_models_git_update_body_auth_method,
)
from ..models.models_git_update_body_git_service_provider import (
    ModelsGitUpdateBodyGitServiceProvider,
    check_models_git_update_body_git_service_provider,
)
from ..models.models_git_update_body_require_pull_request import (
    ModelsGitUpdateBodyRequirePullRequest,
    check_models_git_update_body_require_pull_request,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelsGitUpdateBody")


@_attrs_define
class ModelsGitUpdateBody:
    """
    Attributes:
        auth_method (ModelsGitUpdateBodyAuthMethod | Unset): Authentication method to change to. Example: ssh.
        base_branch (str | Unset): The target branch for Omni pull requests Example: main.
        branch_per_pull_request (bool | Unset): If true, all pull requests will create a branch in Omni
        clone_url (str | Unset): Clone URL of the git repository (SSH or HTTPS). Example: git@github.com:org/repo.git.
        deploy_key_passphrase (str | Unset): Passphrase for deployPrivateKey when it is encrypted. Omni uses it once to
            decrypt the key, then stores the key under its own encryption at rest; the passphrase itself is not retained.
        deploy_private_key (str | Unset): Bring-your-own SSH deploy private key in PEM format (RSA or ED25519, as
            produced by ssh-keygen), used instead of an Omni-generated keypair. On update it replaces the current key,
            enabling zero-downtime rotation: authorize the matching public key with your git provider first, then set it
            here. Only valid for SSH auth.
        git_follower (bool | Unset): If true, the shared model will be read-only
        git_service_provider (ModelsGitUpdateBodyGitServiceProvider | Unset): The git provider type Example: github.
        model_path (str | Unset): Path to model files in the repository Example: my_model.
        require_pull_request (ModelsGitUpdateBodyRequirePullRequest | Unset): Controls when pull requests are required
            Example: users-only.
        ssh_url (str | Unset): Deprecated — use cloneUrl. Clone URL of the git repository. Example:
            git@github.com:org/repo.git.
        token (str | Unset): HTTPS token for authentication (deploy token value, PAT, etc.).
        web_url (str | Unset): Custom web URL for the git repository. Use when the clone URL goes through a tunnel/VPC
            and differs from the inferred HTTPS address Example: https://github.com/org/repo.
    """

    auth_method: ModelsGitUpdateBodyAuthMethod | Unset = UNSET
    base_branch: str | Unset = UNSET
    branch_per_pull_request: bool | Unset = UNSET
    clone_url: str | Unset = UNSET
    deploy_key_passphrase: str | Unset = UNSET
    deploy_private_key: str | Unset = UNSET
    git_follower: bool | Unset = UNSET
    git_service_provider: ModelsGitUpdateBodyGitServiceProvider | Unset = UNSET
    model_path: str | Unset = UNSET
    require_pull_request: ModelsGitUpdateBodyRequirePullRequest | Unset = UNSET
    ssh_url: str | Unset = UNSET
    token: str | Unset = UNSET
    web_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        auth_method: str | Unset = UNSET
        if not isinstance(self.auth_method, Unset):
            auth_method = self.auth_method

        base_branch = self.base_branch

        branch_per_pull_request = self.branch_per_pull_request

        clone_url = self.clone_url

        deploy_key_passphrase = self.deploy_key_passphrase

        deploy_private_key = self.deploy_private_key

        git_follower = self.git_follower

        git_service_provider: str | Unset = UNSET
        if not isinstance(self.git_service_provider, Unset):
            git_service_provider = self.git_service_provider

        model_path = self.model_path

        require_pull_request: str | Unset = UNSET
        if not isinstance(self.require_pull_request, Unset):
            require_pull_request = self.require_pull_request

        ssh_url = self.ssh_url

        token = self.token

        web_url = self.web_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if auth_method is not UNSET:
            field_dict["authMethod"] = auth_method
        if base_branch is not UNSET:
            field_dict["baseBranch"] = base_branch
        if branch_per_pull_request is not UNSET:
            field_dict["branchPerPullRequest"] = branch_per_pull_request
        if clone_url is not UNSET:
            field_dict["cloneUrl"] = clone_url
        if deploy_key_passphrase is not UNSET:
            field_dict["deployKeyPassphrase"] = deploy_key_passphrase
        if deploy_private_key is not UNSET:
            field_dict["deployPrivateKey"] = deploy_private_key
        if git_follower is not UNSET:
            field_dict["gitFollower"] = git_follower
        if git_service_provider is not UNSET:
            field_dict["gitServiceProvider"] = git_service_provider
        if model_path is not UNSET:
            field_dict["modelPath"] = model_path
        if require_pull_request is not UNSET:
            field_dict["requirePullRequest"] = require_pull_request
        if ssh_url is not UNSET:
            field_dict["sshUrl"] = ssh_url
        if token is not UNSET:
            field_dict["token"] = token
        if web_url is not UNSET:
            field_dict["webUrl"] = web_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _auth_method = d.pop("authMethod", UNSET)
        auth_method: ModelsGitUpdateBodyAuthMethod | Unset
        if isinstance(_auth_method, Unset):
            auth_method = UNSET
        else:
            auth_method = check_models_git_update_body_auth_method(_auth_method)

        base_branch = d.pop("baseBranch", UNSET)

        branch_per_pull_request = d.pop("branchPerPullRequest", UNSET)

        clone_url = d.pop("cloneUrl", UNSET)

        deploy_key_passphrase = d.pop("deployKeyPassphrase", UNSET)

        deploy_private_key = d.pop("deployPrivateKey", UNSET)

        git_follower = d.pop("gitFollower", UNSET)

        _git_service_provider = d.pop("gitServiceProvider", UNSET)
        git_service_provider: ModelsGitUpdateBodyGitServiceProvider | Unset
        if isinstance(_git_service_provider, Unset):
            git_service_provider = UNSET
        else:
            git_service_provider = check_models_git_update_body_git_service_provider(_git_service_provider)

        model_path = d.pop("modelPath", UNSET)

        _require_pull_request = d.pop("requirePullRequest", UNSET)
        require_pull_request: ModelsGitUpdateBodyRequirePullRequest | Unset
        if isinstance(_require_pull_request, Unset):
            require_pull_request = UNSET
        else:
            require_pull_request = check_models_git_update_body_require_pull_request(_require_pull_request)

        ssh_url = d.pop("sshUrl", UNSET)

        token = d.pop("token", UNSET)

        web_url = d.pop("webUrl", UNSET)

        models_git_update_body = cls(
            auth_method=auth_method,
            base_branch=base_branch,
            branch_per_pull_request=branch_per_pull_request,
            clone_url=clone_url,
            deploy_key_passphrase=deploy_key_passphrase,
            deploy_private_key=deploy_private_key,
            git_follower=git_follower,
            git_service_provider=git_service_provider,
            model_path=model_path,
            require_pull_request=require_pull_request,
            ssh_url=ssh_url,
            token=token,
            web_url=web_url,
        )

        models_git_update_body.additional_properties = d
        return models_git_update_body

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
