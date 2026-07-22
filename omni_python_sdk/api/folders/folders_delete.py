from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.folders_delete_response import FoldersDeleteResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    folder_id: UUID,
    *,
    force: bool | None | Unset = False,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_force: bool | None | Unset
    if isinstance(force, Unset):
        json_force = UNSET
    else:
        json_force = force
    params["force"] = json_force

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/folders/{folder_id}".format(
            folder_id=quote(str(folder_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | FoldersDeleteResponse | None:
    if response.status_code == 200:
        response_200 = FoldersDeleteResponse.from_dict(response.json())

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
) -> Response[Any | FoldersDeleteResponse]:
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
    force: bool | None | Unset = False,
) -> Response[Any | FoldersDeleteResponse]:
    """Delete a folder

     Deletes a folder. By default, non-empty folders (containing documents or sub-folders) return a 400
    error. Pass `force=true` to recursively archive all documents (soft-delete to trash) and permanently
    remove all sub-folders before deleting the target folder. Force delete is limited to 100 total items
    (documents + sub-folders).

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        force (bool | None | Unset): When true, recursively deletes all documents (sent to trash)
            and sub-folders within the folder. Limited to 100 total items (documents + sub-folders).
            Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersDeleteResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        force=force,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    force: bool | None | Unset = False,
) -> Any | FoldersDeleteResponse | None:
    """Delete a folder

     Deletes a folder. By default, non-empty folders (containing documents or sub-folders) return a 400
    error. Pass `force=true` to recursively archive all documents (soft-delete to trash) and permanently
    remove all sub-folders before deleting the target folder. Force delete is limited to 100 total items
    (documents + sub-folders).

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        force (bool | None | Unset): When true, recursively deletes all documents (sent to trash)
            and sub-folders within the folder. Limited to 100 total items (documents + sub-folders).
            Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersDeleteResponse
    """

    return sync_detailed(
        folder_id=folder_id,
        client=client,
        force=force,
    ).parsed


async def asyncio_detailed(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    force: bool | None | Unset = False,
) -> Response[Any | FoldersDeleteResponse]:
    """Delete a folder

     Deletes a folder. By default, non-empty folders (containing documents or sub-folders) return a 400
    error. Pass `force=true` to recursively archive all documents (soft-delete to trash) and permanently
    remove all sub-folders before deleting the target folder. Force delete is limited to 100 total items
    (documents + sub-folders).

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        force (bool | None | Unset): When true, recursively deletes all documents (sent to trash)
            and sub-folders within the folder. Limited to 100 total items (documents + sub-folders).
            Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersDeleteResponse]
    """

    kwargs = _get_kwargs(
        folder_id=folder_id,
        force=force,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    folder_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    force: bool | None | Unset = False,
) -> Any | FoldersDeleteResponse | None:
    """Delete a folder

     Deletes a folder. By default, non-empty folders (containing documents or sub-folders) return a 400
    error. Pass `force=true` to recursively archive all documents (soft-delete to trash) and permanently
    remove all sub-folders before deleting the target folder. Force delete is limited to 100 total items
    (documents + sub-folders).

    Args:
        folder_id (UUID): Unique identifier for the folder Example:
            550e8400-e29b-41d4-a716-446655440000.
        force (bool | None | Unset): When true, recursively deletes all documents (sent to trash)
            and sub-folders within the folder. Limited to 100 total items (documents + sub-folders).
            Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersDeleteResponse
    """

    return (
        await asyncio_detailed(
            folder_id=folder_id,
            client=client,
            force=force,
        )
    ).parsed
