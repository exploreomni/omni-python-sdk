from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_create_connections_create_body import ConnectionsCreateConnectionsCreateBody
from ...models.connections_create_connections_create_response import ConnectionsCreateConnectionsCreateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: ConnectionsCreateConnectionsCreateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/connections",
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ConnectionsCreateConnectionsCreateResponse | None:
    if response.status_code == 201:
        response_201 = ConnectionsCreateConnectionsCreateResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ConnectionsCreateConnectionsCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsCreateConnectionsCreateBody | Unset = UNSET,
) -> Response[Any | ConnectionsCreateConnectionsCreateResponse]:
    """Create connection

     Create a new database connection. The request body varies by dialect - see dialect-specific
    documentation for required fields.

    Args:
        body (ConnectionsCreateConnectionsCreateBody | Unset): Request body for creating a
            database connection. Required fields: dialect, name, passwordUnencrypted. Additional
            fields may be required depending on the dialect.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsCreateConnectionsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsCreateConnectionsCreateBody | Unset = UNSET,
) -> Any | ConnectionsCreateConnectionsCreateResponse | None:
    """Create connection

     Create a new database connection. The request body varies by dialect - see dialect-specific
    documentation for required fields.

    Args:
        body (ConnectionsCreateConnectionsCreateBody | Unset): Request body for creating a
            database connection. Required fields: dialect, name, passwordUnencrypted. Additional
            fields may be required depending on the dialect.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsCreateConnectionsCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsCreateConnectionsCreateBody | Unset = UNSET,
) -> Response[Any | ConnectionsCreateConnectionsCreateResponse]:
    """Create connection

     Create a new database connection. The request body varies by dialect - see dialect-specific
    documentation for required fields.

    Args:
        body (ConnectionsCreateConnectionsCreateBody | Unset): Request body for creating a
            database connection. Required fields: dialect, name, passwordUnencrypted. Additional
            fields may be required depending on the dialect.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsCreateConnectionsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsCreateConnectionsCreateBody | Unset = UNSET,
) -> Any | ConnectionsCreateConnectionsCreateResponse | None:
    """Create connection

     Create a new database connection. The request body varies by dialect - see dialect-specific
    documentation for required fields.

    Args:
        body (ConnectionsCreateConnectionsCreateBody | Unset): Request body for creating a
            database connection. Required fields: dialect, name, passwordUnencrypted. Additional
            fields may be required depending on the dialect.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsCreateConnectionsCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
