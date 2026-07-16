from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.user_groups_get_model_roles_response import UserGroupsGetModelRolesResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: str,
    *,
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_connection_id: str | Unset = UNSET
    if not isinstance(connection_id, Unset):
        json_connection_id = str(connection_id)
    params["connectionId"] = json_connection_id

    json_model_id: str | Unset = UNSET
    if not isinstance(model_id, Unset):
        json_model_id = str(model_id)
    params["modelId"] = json_model_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/user-groups/{id}/model-roles".format(
            id=quote(str(id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | UserGroupsGetModelRolesResponse | None:
    if response.status_code == 200:
        response_200 = UserGroupsGetModelRolesResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | UserGroupsGetModelRolesResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    id: str,
    *,
    client: AuthenticatedClient | Client,
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
) -> Response[Any | UserGroupsGetModelRolesResponse]:
    """Get user group model roles

    Args:
        id (str): User group short identifier (miniUuid) Example: abc123.
        connection_id (UUID | Unset): Filter results to a specific connection Example:
            550e8400-e29b-41d4-a716-446655440000.
        model_id (UUID | Unset): Filter results to a specific model Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UserGroupsGetModelRolesResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        connection_id=connection_id,
        model_id=model_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: str,
    *,
    client: AuthenticatedClient | Client,
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
) -> Any | UserGroupsGetModelRolesResponse | None:
    """Get user group model roles

    Args:
        id (str): User group short identifier (miniUuid) Example: abc123.
        connection_id (UUID | Unset): Filter results to a specific connection Example:
            550e8400-e29b-41d4-a716-446655440000.
        model_id (UUID | Unset): Filter results to a specific model Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UserGroupsGetModelRolesResponse
    """

    return sync_detailed(
        id=id,
        client=client,
        connection_id=connection_id,
        model_id=model_id,
    ).parsed


async def asyncio_detailed(
    id: str,
    *,
    client: AuthenticatedClient | Client,
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
) -> Response[Any | UserGroupsGetModelRolesResponse]:
    """Get user group model roles

    Args:
        id (str): User group short identifier (miniUuid) Example: abc123.
        connection_id (UUID | Unset): Filter results to a specific connection Example:
            550e8400-e29b-41d4-a716-446655440000.
        model_id (UUID | Unset): Filter results to a specific model Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UserGroupsGetModelRolesResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        connection_id=connection_id,
        model_id=model_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: str,
    *,
    client: AuthenticatedClient | Client,
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
) -> Any | UserGroupsGetModelRolesResponse | None:
    """Get user group model roles

    Args:
        id (str): User group short identifier (miniUuid) Example: abc123.
        connection_id (UUID | Unset): Filter results to a specific connection Example:
            550e8400-e29b-41d4-a716-446655440000.
        model_id (UUID | Unset): Filter results to a specific model Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UserGroupsGetModelRolesResponse
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            connection_id=connection_id,
            model_id=model_id,
        )
    ).parsed
