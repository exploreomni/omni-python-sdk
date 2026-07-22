from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_dbt_environments_list_sort_direction import (
    ConnectionsDbtEnvironmentsListSortDirection,
)
from ...models.connections_dbt_environments_list_sort_field import (
    ConnectionsDbtEnvironmentsListSortField,
)
from ...models.dbt_environment_list_response import DbtEnvironmentListResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    connection_id: UUID,
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ConnectionsDbtEnvironmentsListSortDirection | Unset = "desc",
    sort_field: ConnectionsDbtEnvironmentsListSortField | Unset = "name",
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    json_sort_field: str | Unset = UNSET
    if not isinstance(sort_field, Unset):
        json_sort_field = sort_field

    params["sortField"] = json_sort_field

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/connections/{connection_id}/dbt/environments".format(
            connection_id=quote(str(connection_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DbtEnvironmentListResponse | None:
    if response.status_code == 200:
        response_200 = DbtEnvironmentListResponse.from_dict(response.json())

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
) -> Response[Any | DbtEnvironmentListResponse]:
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
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ConnectionsDbtEnvironmentsListSortDirection | Unset = "desc",
    sort_field: ConnectionsDbtEnvironmentsListSortField | Unset = "name",
) -> Response[Any | DbtEnvironmentListResponse]:
    """List dbt environments

     List all dbt environments for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ConnectionsDbtEnvironmentsListSortDirection | Unset): Sort direction for
            results Default: 'desc'. Example: desc.
        sort_field (ConnectionsDbtEnvironmentsListSortField | Unset): Field to sort results by
            Default: 'name'. Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentListResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ConnectionsDbtEnvironmentsListSortDirection | Unset = "desc",
    sort_field: ConnectionsDbtEnvironmentsListSortField | Unset = "name",
) -> Any | DbtEnvironmentListResponse | None:
    """List dbt environments

     List all dbt environments for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ConnectionsDbtEnvironmentsListSortDirection | Unset): Sort direction for
            results Default: 'desc'. Example: desc.
        sort_field (ConnectionsDbtEnvironmentsListSortField | Unset): Field to sort results by
            Default: 'name'. Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentListResponse
    """

    return sync_detailed(
        connection_id=connection_id,
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    ).parsed


async def asyncio_detailed(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ConnectionsDbtEnvironmentsListSortDirection | Unset = "desc",
    sort_field: ConnectionsDbtEnvironmentsListSortField | Unset = "name",
) -> Response[Any | DbtEnvironmentListResponse]:
    """List dbt environments

     List all dbt environments for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ConnectionsDbtEnvironmentsListSortDirection | Unset): Sort direction for
            results Default: 'desc'. Example: desc.
        sort_field (ConnectionsDbtEnvironmentsListSortField | Unset): Field to sort results by
            Default: 'name'. Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DbtEnvironmentListResponse]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ConnectionsDbtEnvironmentsListSortDirection | Unset = "desc",
    sort_field: ConnectionsDbtEnvironmentsListSortField | Unset = "name",
) -> Any | DbtEnvironmentListResponse | None:
    """List dbt environments

     List all dbt environments for a connection.

    Args:
        connection_id (UUID): Connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ConnectionsDbtEnvironmentsListSortDirection | Unset): Sort direction for
            results Default: 'desc'. Example: desc.
        sort_field (ConnectionsDbtEnvironmentsListSortField | Unset): Field to sort results by
            Default: 'name'. Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DbtEnvironmentListResponse
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
        )
    ).parsed
