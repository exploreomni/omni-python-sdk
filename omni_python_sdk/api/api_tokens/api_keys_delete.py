from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_key_delete_response import ApiKeyDeleteResponse
from ...types import Response


def _get_kwargs(
    id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/api-keys/{id}".format(
            id=quote(str(id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ApiKeyDeleteResponse | None:
    if response.status_code == 200:
        response_200 = ApiKeyDeleteResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if response.status_code == 405:
        response_405 = cast(Any, None)
        return response_405

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ApiKeyDeleteResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | ApiKeyDeleteResponse]:
    """Revoke an API token

     Revokes an API token by permanently deleting it. Works for all token types. Requires organization
    admin permissions.

    Args:
        id (UUID): Token UUID Example: a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ApiKeyDeleteResponse]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | ApiKeyDeleteResponse | None:
    """Revoke an API token

     Revokes an API token by permanently deleting it. Works for all token types. Requires organization
    admin permissions.

    Args:
        id (UUID): Token UUID Example: a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ApiKeyDeleteResponse
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | ApiKeyDeleteResponse]:
    """Revoke an API token

     Revokes an API token by permanently deleting it. Works for all token types. Requires organization
    admin permissions.

    Args:
        id (UUID): Token UUID Example: a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ApiKeyDeleteResponse]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | ApiKeyDeleteResponse | None:
    """Revoke an API token

     Revokes an API token by permanently deleting it. Works for all token types. Requires organization
    admin permissions.

    Args:
        id (UUID): Token UUID Example: a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ApiKeyDeleteResponse
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
