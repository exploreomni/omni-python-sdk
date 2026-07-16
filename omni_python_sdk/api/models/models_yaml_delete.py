from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_yaml_delete_mode import ModelsYamlDeleteMode
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    branch_id: UUID | Unset = UNSET,
    file_name: str,
    mode: ModelsYamlDeleteMode | Unset = "combined",
    commit_message: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branchId"] = json_branch_id

    json_file_name: str
    json_file_name = file_name
    params["fileName"] = json_file_name

    json_mode: str | Unset = UNSET
    if not isinstance(mode, Unset):
        json_mode = mode

    params["mode"] = json_mode

    params["commitMessage"] = commit_message

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/models/{model_id}/yaml".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | None:
    if response.status_code == 200:
        return None

    if response.status_code == 400:
        return None

    if response.status_code == 401:
        return None

    if response.status_code == 403:
        return None

    if response.status_code == 404:
        return None

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    file_name: str,
    mode: ModelsYamlDeleteMode | Unset = "combined",
    commit_message: str | Unset = UNSET,
) -> Response[Any]:
    """Delete model YAML file

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str): File name to delete (must end with '.topic' or '.view')
        mode (ModelsYamlDeleteMode | Unset): IDE mode for YAML operations Default: 'combined'.
        commit_message (str | Unset): Commit message for git sync

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        file_name=file_name,
        mode=mode,
        commit_message=commit_message,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    file_name: str,
    mode: ModelsYamlDeleteMode | Unset = "combined",
    commit_message: str | Unset = UNSET,
) -> Response[Any]:
    """Delete model YAML file

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str): File name to delete (must end with '.topic' or '.view')
        mode (ModelsYamlDeleteMode | Unset): IDE mode for YAML operations Default: 'combined'.
        commit_message (str | Unset): Commit message for git sync

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        file_name=file_name,
        mode=mode,
        commit_message=commit_message,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
