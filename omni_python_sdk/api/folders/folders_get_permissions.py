from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.folders_get_permissions_response import FoldersGetPermissionsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    folder_id: UUID,
    *,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/folders/{folder_id}/permissions".format(
            folder_id=quote(str(folder_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | FoldersGetPermissionsResponse | None:
    if response.status_code == 200:
        response_200 = FoldersGetPermissionsResponse.from_dict(response.json())

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
) -> Response[Any | FoldersGetPermissionsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | FoldersGetPermissionsResponse]:
    """Get folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        user_id (UUID | Unset): Filter permits for a specific user. If omitted, returns all
            permits (requires MANAGER role).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersGetPermissionsResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | FoldersGetPermissionsResponse | None:
    """Get folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        user_id (UUID | Unset): Filter permits for a specific user. If omitted, returns all
            permits (requires MANAGER role).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersGetPermissionsResponse
    """

    return sync_detailed(
        folder_id=folder_id,
        client=client,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | FoldersGetPermissionsResponse]:
    """Get folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        user_id (UUID | Unset): Filter permits for a specific user. If omitted, returns all
            permits (requires MANAGER role).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersGetPermissionsResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | FoldersGetPermissionsResponse | None:
    """Get folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        user_id (UUID | Unset): Filter permits for a specific user. If omitted, returns all
            permits (requires MANAGER role).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersGetPermissionsResponse
    """

    return (
        await asyncio_detailed(
            folder_id=folder_id,
            client=client,
            user_id=user_id,
        )
    ).parsed
