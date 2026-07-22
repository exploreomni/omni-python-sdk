from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_get_permissions_response import DocumentsGetPermissionsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
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
        "url": "/api/v1/documents/{identifier}/permissions".format(
            identifier=quote(str(identifier), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsGetPermissionsResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsGetPermissionsResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsGetPermissionsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsGetPermissionsResponse]:
    r"""Get document permissions

     Returns the document-level ability values (the Share dialog \"Abilities\" toggles), plus the
    resolved permits for a specific user when `userId` is provided.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): User membership ID to check permissions for. When omitted, only
            the document-level abilities are returned.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsGetPermissionsResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsGetPermissionsResponse | None:
    r"""Get document permissions

     Returns the document-level ability values (the Share dialog \"Abilities\" toggles), plus the
    resolved permits for a specific user when `userId` is provided.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): User membership ID to check permissions for. When omitted, only
            the document-level abilities are returned.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsGetPermissionsResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsGetPermissionsResponse]:
    r"""Get document permissions

     Returns the document-level ability values (the Share dialog \"Abilities\" toggles), plus the
    resolved permits for a specific user when `userId` is provided.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): User membership ID to check permissions for. When omitted, only
            the document-level abilities are returned.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsGetPermissionsResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsGetPermissionsResponse | None:
    r"""Get document permissions

     Returns the document-level ability values (the Share dialog \"Abilities\" toggles), plus the
    resolved permits for a specific user when `userId` is provided.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): User membership ID to check permissions for. When omitted, only
            the document-level abilities are returned.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsGetPermissionsResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            user_id=user_id,
        )
    ).parsed
