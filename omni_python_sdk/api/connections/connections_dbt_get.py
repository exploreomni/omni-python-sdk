from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_dbt_get_dbt_configured_response import ConnectionsDbtGetDbtConfiguredResponse
from ...models.connections_dbt_get_dbt_not_configured_response import ConnectionsDbtGetDbtNotConfiguredResponse
from ...types import Response


def _get_kwargs(
    connection_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/connections/{connection_id}/dbt".format(
            connection_id=quote(str(connection_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse | None:
    if response.status_code == 200:

        def _parse_response_200(
            data: object,
        ) -> ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_200_dbt_configured_response = ConnectionsDbtGetDbtConfiguredResponse.from_dict(data)

                return response_200_dbt_configured_response
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_200_dbt_not_configured_response = ConnectionsDbtGetDbtNotConfiguredResponse.from_dict(data)

            return response_200_dbt_not_configured_response

        response_200 = _parse_response_200(response.json())

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
) -> Response[Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse]:
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
) -> Response[Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse]:
    """Get dbt configuration

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse | None:
    """Get dbt configuration

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse
    """

    return sync_detailed(
        connection_id=connection_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse]:
    """Get dbt configuration

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse | None:
    """Get dbt configuration

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsDbtGetDbtConfiguredResponse | ConnectionsDbtGetDbtNotConfiguredResponse
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            client=client,
        )
    ).parsed
