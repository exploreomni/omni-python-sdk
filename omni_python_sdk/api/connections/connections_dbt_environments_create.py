from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dbt_environment_create_body import DbtEnvironmentCreateBody
from ...models.dbt_environment_item import DbtEnvironmentItem
from ...types import Response


def _get_kwargs(
    connection_id: UUID,
    *,
    body: DbtEnvironmentCreateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/connections/{connection_id}/dbt/environments".format(
            connection_id=quote(str(connection_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DbtEnvironmentItem | None:
    if response.status_code == 201:
        response_201 = DbtEnvironmentItem.from_dict(response.json())

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
) -> Response[Any | DbtEnvironmentItem]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: DbtEnvironmentCreateBody,
) -> Response[Any | DbtEnvironmentItem]:
    """Create dbt environment

     Create a new dbt environment for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (DbtEnvironmentCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentItem]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: DbtEnvironmentCreateBody,
) -> Any | DbtEnvironmentItem | None:
    """Create dbt environment

     Create a new dbt environment for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (DbtEnvironmentCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentItem
    """

    return sync_detailed(
        connection_id=connection_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: DbtEnvironmentCreateBody,
) -> Response[Any | DbtEnvironmentItem]:
    """Create dbt environment

     Create a new dbt environment for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (DbtEnvironmentCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentItem]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: DbtEnvironmentCreateBody,
) -> Any | DbtEnvironmentItem | None:
    """Create dbt environment

     Create a new dbt environment for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        body (DbtEnvironmentCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentItem
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            client=client,
            body=body,
        )
    ).parsed
