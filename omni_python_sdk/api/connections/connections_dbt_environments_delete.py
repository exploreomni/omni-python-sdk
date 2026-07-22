from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dbt_environment_delete_response import DbtEnvironmentDeleteResponse
from ...types import Response


def _get_kwargs(
    connection_id: UUID,
    environment_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/connections/{connection_id}/dbt/environments/{environment_id}".format(
            connection_id=quote(str(connection_id), safe=""),
            environment_id=quote(str(environment_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DbtEnvironmentDeleteResponse | None:
    if response.status_code == 200:
        response_200 = DbtEnvironmentDeleteResponse.from_dict(response.json())

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
) -> Response[Any | DbtEnvironmentDeleteResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    connection_id: UUID,
    environment_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DbtEnvironmentDeleteResponse]:
    """Delete dbt environment

     Delete a dbt environment from a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        environment_id (UUID): Environment ID Example: 247dc6dc-2a58-4688-9521-c5ed3e99c1e8.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentDeleteResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        environment_id=environment_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: UUID,
    environment_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DbtEnvironmentDeleteResponse | None:
    """Delete dbt environment

     Delete a dbt environment from a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        environment_id (UUID): Environment ID Example: 247dc6dc-2a58-4688-9521-c5ed3e99c1e8.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentDeleteResponse
    """

    return sync_detailed(
        connection_id=connection_id,
        environment_id=environment_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    connection_id: UUID,
    environment_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DbtEnvironmentDeleteResponse]:
    """Delete dbt environment

     Delete a dbt environment from a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        environment_id (UUID): Environment ID Example: 247dc6dc-2a58-4688-9521-c5ed3e99c1e8.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentDeleteResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        environment_id=environment_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: UUID,
    environment_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DbtEnvironmentDeleteResponse | None:
    """Delete dbt environment

     Delete a dbt environment from a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        environment_id (UUID): Environment ID Example: 247dc6dc-2a58-4688-9521-c5ed3e99c1e8.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentDeleteResponse
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            environment_id=environment_id,
            client=client,
        )
    ).parsed
