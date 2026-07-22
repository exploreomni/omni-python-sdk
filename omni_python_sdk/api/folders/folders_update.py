from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.folders_update_body import FoldersUpdateBody
from ...models.folders_update_response import FoldersUpdateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    folder_id: UUID,
    *,
    body: FoldersUpdateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/folders/{folder_id}".format(
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
) -> Any | FoldersUpdateResponse | None:
    if response.status_code == 200:
        response_200 = FoldersUpdateResponse.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | FoldersUpdateResponse]:
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
    body: FoldersUpdateBody | Unset = UNSET,
) -> Response[Any | FoldersUpdateResponse]:
    """Update a folder

     Update a folder's display name and/or URL path segment. At least one of `name` or `path` must be
    provided. Changing the name does not automatically update the path. When the path is updated,
    descendant folder paths are cascaded.

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersUpdateResponse]
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
    body: FoldersUpdateBody | Unset = UNSET,
) -> Any | FoldersUpdateResponse | None:
    """Update a folder

     Update a folder's display name and/or URL path segment. At least one of `name` or `path` must be
    provided. Changing the name does not automatically update the path. When the path is updated,
    descendant folder paths are cascaded.

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersUpdateResponse
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
    body: FoldersUpdateBody | Unset = UNSET,
) -> Response[Any | FoldersUpdateResponse]:
    """Update a folder

     Update a folder's display name and/or URL path segment. At least one of `name` or `path` must be
    provided. Changing the name does not automatically update the path. When the path is updated,
    descendant folder paths are cascaded.

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersUpdateResponse]
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
    body: FoldersUpdateBody | Unset = UNSET,
) -> Any | FoldersUpdateResponse | None:
    """Update a folder

     Update a folder's display name and/or URL path segment. At least one of `name` or `path` must be
    provided. Changing the name does not automatically update the path. When the path is updated,
    descendant folder paths are cascaded.

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (FoldersUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersUpdateResponse
    """

    return (
        await asyncio_detailed(
            folder_id=folder_id,
            client=client,
            body=body,
        )
    ).parsed
