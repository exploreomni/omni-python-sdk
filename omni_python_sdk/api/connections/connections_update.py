from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_update_connections_update_body import ConnectionsUpdateConnectionsUpdateBody
from ...models.connections_update_connections_update_response import ConnectionsUpdateConnectionsUpdateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: UUID,
    *,
    body: ConnectionsUpdateConnectionsUpdateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/connections/{id}".format(
            id=quote(str(id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ConnectionsUpdateConnectionsUpdateResponse | None:
    if response.status_code == 200:
        response_200 = ConnectionsUpdateConnectionsUpdateResponse.from_dict(response.json())

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
) -> Response[Any | ConnectionsUpdateConnectionsUpdateResponse]:
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
    body: ConnectionsUpdateConnectionsUpdateBody | Unset = UNSET,
) -> Response[Any | ConnectionsUpdateConnectionsUpdateResponse]:
    """Update connection

     Update connection settings including base role, environment user attributes, and credentials.

    Credential fields:
    - `passwordUnencrypted`: Update password (all dialects) or service account JSON (BigQuery)
    - `privateKey`: Add/rotate RSA keypair for Snowflake keypair authentication

    Note: Credentials are encrypted at rest and never returned in API responses.

    Args:
        id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (ConnectionsUpdateConnectionsUpdateBody | Unset): Request body for updating
            connection attributes and credentials. At least one field must be provided.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsUpdateConnectionsUpdateResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsUpdateConnectionsUpdateBody | Unset = UNSET,
) -> Any | ConnectionsUpdateConnectionsUpdateResponse | None:
    """Update connection

     Update connection settings including base role, environment user attributes, and credentials.

    Credential fields:
    - `passwordUnencrypted`: Update password (all dialects) or service account JSON (BigQuery)
    - `privateKey`: Add/rotate RSA keypair for Snowflake keypair authentication

    Note: Credentials are encrypted at rest and never returned in API responses.

    Args:
        id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (ConnectionsUpdateConnectionsUpdateBody | Unset): Request body for updating
            connection attributes and credentials. At least one field must be provided.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsUpdateConnectionsUpdateResponse
    """

    return sync_detailed(
        id=id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsUpdateConnectionsUpdateBody | Unset = UNSET,
) -> Response[Any | ConnectionsUpdateConnectionsUpdateResponse]:
    """Update connection

     Update connection settings including base role, environment user attributes, and credentials.

    Credential fields:
    - `passwordUnencrypted`: Update password (all dialects) or service account JSON (BigQuery)
    - `privateKey`: Add/rotate RSA keypair for Snowflake keypair authentication

    Note: Credentials are encrypted at rest and never returned in API responses.

    Args:
        id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (ConnectionsUpdateConnectionsUpdateBody | Unset): Request body for updating
            connection attributes and credentials. At least one field must be provided.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsUpdateConnectionsUpdateResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ConnectionsUpdateConnectionsUpdateBody | Unset = UNSET,
) -> Any | ConnectionsUpdateConnectionsUpdateResponse | None:
    """Update connection

     Update connection settings including base role, environment user attributes, and credentials.

    Credential fields:
    - `passwordUnencrypted`: Update password (all dialects) or service account JSON (BigQuery)
    - `privateKey`: Add/rotate RSA keypair for Snowflake keypair authentication

    Note: Credentials are encrypted at rest and never returned in API responses.

    Args:
        id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (ConnectionsUpdateConnectionsUpdateBody | Unset): Request body for updating
            connection attributes and credentials. At least one field must be provided.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsUpdateConnectionsUpdateResponse
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            body=body,
        )
    ).parsed
