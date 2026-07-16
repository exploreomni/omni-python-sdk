from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.folders_revoke_permissions_body import FoldersRevokePermissionsBody
from ...models.folders_revoke_permissions_response import FoldersRevokePermissionsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    folder_id: UUID,
    *,
    body: FoldersRevokePermissionsBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/folders/{folder_id}/permissions".format(
            folder_id=quote(str(folder_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | FoldersRevokePermissionsResponse | None:
    if response.status_code == 200:
        response_200 = FoldersRevokePermissionsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

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
) -> Response[Any | FoldersRevokePermissionsResponse]:
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
    body: FoldersRevokePermissionsBody | Unset = UNSET,
) -> Response[Any | FoldersRevokePermissionsResponse]:
    """Revoke folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersRevokePermissionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersRevokePermissionsResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: FoldersRevokePermissionsBody | Unset = UNSET,
) -> Any | FoldersRevokePermissionsResponse | None:
    """Revoke folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersRevokePermissionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersRevokePermissionsResponse
    """

    return sync_detailed(
        folder_id=folder_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: FoldersRevokePermissionsBody | Unset = UNSET,
) -> Response[Any | FoldersRevokePermissionsResponse]:
    """Revoke folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersRevokePermissionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersRevokePermissionsResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: FoldersRevokePermissionsBody | Unset = UNSET,
) -> Any | FoldersRevokePermissionsResponse | None:
    """Revoke folder permissions

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersRevokePermissionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersRevokePermissionsResponse
    """

    return (
        await asyncio_detailed(
            folder_id=folder_id,
            client=client,
            body=body,
        )
    ).parsed
