from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connections_list_connections_list_response import ConnectionsListConnectionsListResponse
from ...models.connections_list_sort_direction import (
    ConnectionsListSortDirection,
)
from ...models.connections_list_sort_field import ConnectionsListSortField
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    database: str | Unset = UNSET,
    dialect: str | Unset = UNSET,
    include_deleted: bool | Unset = UNSET,
    name: str | Unset = UNSET,
    sort_direction: ConnectionsListSortDirection | Unset = UNSET,
    sort_field: ConnectionsListSortField | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["database"] = database

    params["dialect"] = dialect

    params["includeDeleted"] = include_deleted

    params["name"] = name

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
        "url": "/api/v1/connections",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ConnectionsListConnectionsListResponse | None:
    if response.status_code == 200:
        response_200 = ConnectionsListConnectionsListResponse.from_dict(response.json())

        return response_200

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
) -> Response[Any | ConnectionsListConnectionsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    database: str | Unset = UNSET,
    dialect: str | Unset = UNSET,
    include_deleted: bool | Unset = UNSET,
    name: str | Unset = UNSET,
    sort_direction: ConnectionsListSortDirection | Unset = UNSET,
    sort_field: ConnectionsListSortField | Unset = UNSET,
) -> Response[Any | ConnectionsListConnectionsListResponse]:
    """List connections

    Args:
        database (str | Unset): Filter by database name (case-insensitive contains) Example:
            analytics.
        dialect (str | Unset): Filter by dialect(s). Comma-separated list for multiple values
            Example: snowflake,bigquery.
        include_deleted (bool | Unset): Include soft-deleted connections in results
        name (str | Unset): Filter by connection name (case-insensitive contains) Example:
            Production.
        sort_direction (ConnectionsListSortDirection | Unset): Sort direction Example: desc.
        sort_field (ConnectionsListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsListConnectionsListResponse]
    """

    kwargs = _get_kwargs(
        database=database,
        dialect=dialect,
        include_deleted=include_deleted,
        name=name,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    database: str | Unset = UNSET,
    dialect: str | Unset = UNSET,
    include_deleted: bool | Unset = UNSET,
    name: str | Unset = UNSET,
    sort_direction: ConnectionsListSortDirection | Unset = UNSET,
    sort_field: ConnectionsListSortField | Unset = UNSET,
) -> Any | ConnectionsListConnectionsListResponse | None:
    """List connections

    Args:
        database (str | Unset): Filter by database name (case-insensitive contains) Example:
            analytics.
        dialect (str | Unset): Filter by dialect(s). Comma-separated list for multiple values
            Example: snowflake,bigquery.
        include_deleted (bool | Unset): Include soft-deleted connections in results
        name (str | Unset): Filter by connection name (case-insensitive contains) Example:
            Production.
        sort_direction (ConnectionsListSortDirection | Unset): Sort direction Example: desc.
        sort_field (ConnectionsListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsListConnectionsListResponse
    """

    return sync_detailed(
        client=client,
        database=database,
        dialect=dialect,
        include_deleted=include_deleted,
        name=name,
        sort_direction=sort_direction,
        sort_field=sort_field,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    database: str | Unset = UNSET,
    dialect: str | Unset = UNSET,
    include_deleted: bool | Unset = UNSET,
    name: str | Unset = UNSET,
    sort_direction: ConnectionsListSortDirection | Unset = UNSET,
    sort_field: ConnectionsListSortField | Unset = UNSET,
) -> Response[Any | ConnectionsListConnectionsListResponse]:
    """List connections

    Args:
        database (str | Unset): Filter by database name (case-insensitive contains) Example:
            analytics.
        dialect (str | Unset): Filter by dialect(s). Comma-separated list for multiple values
            Example: snowflake,bigquery.
        include_deleted (bool | Unset): Include soft-deleted connections in results
        name (str | Unset): Filter by connection name (case-insensitive contains) Example:
            Production.
        sort_direction (ConnectionsListSortDirection | Unset): Sort direction Example: desc.
        sort_field (ConnectionsListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ConnectionsListConnectionsListResponse]
    """

    kwargs = _get_kwargs(
        database=database,
        dialect=dialect,
        include_deleted=include_deleted,
        name=name,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    database: str | Unset = UNSET,
    dialect: str | Unset = UNSET,
    include_deleted: bool | Unset = UNSET,
    name: str | Unset = UNSET,
    sort_direction: ConnectionsListSortDirection | Unset = UNSET,
    sort_field: ConnectionsListSortField | Unset = UNSET,
) -> Any | ConnectionsListConnectionsListResponse | None:
    """List connections

    Args:
        database (str | Unset): Filter by database name (case-insensitive contains) Example:
            analytics.
        dialect (str | Unset): Filter by dialect(s). Comma-separated list for multiple values
            Example: snowflake,bigquery.
        include_deleted (bool | Unset): Include soft-deleted connections in results
        name (str | Unset): Filter by connection name (case-insensitive contains) Example:
            Production.
        sort_direction (ConnectionsListSortDirection | Unset): Sort direction Example: desc.
        sort_field (ConnectionsListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ConnectionsListConnectionsListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            database=database,
            dialect=dialect,
            include_deleted=include_deleted,
            name=name,
            sort_direction=sort_direction,
            sort_field=sort_field,
        )
    ).parsed
