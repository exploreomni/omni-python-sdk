from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connection_environments_create_connections_environments_create_body import (
    ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody,
)
from ...models.connection_environments_create_connections_environments_create_response import (
    ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/connection-environments",
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse | None:
    if response.status_code == 201:
        response_201 = ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse.from_dict(response.json())

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

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset = UNSET,
) -> Response[Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse]:
    """Create connection environments

    Args:
        body (ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset): Request body
            for creating connection environments

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse]
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
    body: ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset = UNSET,
) -> Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse | None:
    """Create connection environments

    Args:
        body (ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset): Request body
            for creating connection environments

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset = UNSET,
) -> Response[Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse]:
    """Create connection environments

    Args:
        body (ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset): Request body
            for creating connection environments

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset = UNSET,
) -> Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse | None:
    """Create connection environments

    Args:
        body (ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody | Unset): Request body
            for creating connection environments

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
