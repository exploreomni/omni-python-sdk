from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.uploads_list_response import UploadsListResponse
from ...models.uploads_list_sort_direction import UploadsListSortDirection
from ...models.uploads_list_sort_field import UploadsListSortField
from ...models.uploads_list_type import UploadsListType
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: UploadsListSortDirection | Unset = "desc",
    sort_field: UploadsListSortField | Unset = "updatedAt",
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    search_term: str | Unset = UNSET,
    type_: UploadsListType | Unset = "csv",
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

    json_connection_id: str | Unset = UNSET
    if not isinstance(connection_id, Unset):
        json_connection_id = str(connection_id)
    params["connectionId"] = json_connection_id

    json_model_id: str | Unset = UNSET
    if not isinstance(model_id, Unset):
        json_model_id = str(model_id)
    params["modelId"] = json_model_id

    params["searchTerm"] = search_term

    json_type_: str | Unset = UNSET
    if not isinstance(type_, Unset):
        json_type_ = type_

    params["type"] = json_type_

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/uploads",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | UploadsListResponse | None:
    if response.status_code == 200:
        response_200 = UploadsListResponse.from_dict(response.json())

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
) -> Response[Any | UploadsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: UploadsListSortDirection | Unset = "desc",
    sort_field: UploadsListSortField | Unset = "updatedAt",
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    search_term: str | Unset = UNSET,
    type_: UploadsListType | Unset = "csv",
) -> Response[Any | UploadsListResponse]:
    """List uploads

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (UploadsListSortDirection | Unset): Sort direction (default: desc) Default:
            'desc'. Example: desc.
        sort_field (UploadsListSortField | Unset): Field to sort by (default: updatedAt) Default:
            'updatedAt'.
        connection_id (UUID | Unset): Filter by connection ID
        model_id (UUID | Unset): Filter by model ID. Shared models return connection uploads;
            workbook models return their own uploads.
        search_term (str | Unset): Search term to filter by file name
        type_ (UploadsListType | Unset): Filter by upload type (default: csv) Default: 'csv'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UploadsListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        connection_id=connection_id,
        model_id=model_id,
        search_term=search_term,
        type_=type_,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: UploadsListSortDirection | Unset = "desc",
    sort_field: UploadsListSortField | Unset = "updatedAt",
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    search_term: str | Unset = UNSET,
    type_: UploadsListType | Unset = "csv",
) -> Any | UploadsListResponse | None:
    """List uploads

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (UploadsListSortDirection | Unset): Sort direction (default: desc) Default:
            'desc'. Example: desc.
        sort_field (UploadsListSortField | Unset): Field to sort by (default: updatedAt) Default:
            'updatedAt'.
        connection_id (UUID | Unset): Filter by connection ID
        model_id (UUID | Unset): Filter by model ID. Shared models return connection uploads;
            workbook models return their own uploads.
        search_term (str | Unset): Search term to filter by file name
        type_ (UploadsListType | Unset): Filter by upload type (default: csv) Default: 'csv'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UploadsListResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        connection_id=connection_id,
        model_id=model_id,
        search_term=search_term,
        type_=type_,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: UploadsListSortDirection | Unset = "desc",
    sort_field: UploadsListSortField | Unset = "updatedAt",
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    search_term: str | Unset = UNSET,
    type_: UploadsListType | Unset = "csv",
) -> Response[Any | UploadsListResponse]:
    """List uploads

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (UploadsListSortDirection | Unset): Sort direction (default: desc) Default:
            'desc'. Example: desc.
        sort_field (UploadsListSortField | Unset): Field to sort by (default: updatedAt) Default:
            'updatedAt'.
        connection_id (UUID | Unset): Filter by connection ID
        model_id (UUID | Unset): Filter by model ID. Shared models return connection uploads;
            workbook models return their own uploads.
        search_term (str | Unset): Search term to filter by file name
        type_ (UploadsListType | Unset): Filter by upload type (default: csv) Default: 'csv'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UploadsListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        connection_id=connection_id,
        model_id=model_id,
        search_term=search_term,
        type_=type_,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: UploadsListSortDirection | Unset = "desc",
    sort_field: UploadsListSortField | Unset = "updatedAt",
    connection_id: UUID | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    search_term: str | Unset = UNSET,
    type_: UploadsListType | Unset = "csv",
) -> Any | UploadsListResponse | None:
    """List uploads

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (UploadsListSortDirection | Unset): Sort direction (default: desc) Default:
            'desc'. Example: desc.
        sort_field (UploadsListSortField | Unset): Field to sort by (default: updatedAt) Default:
            'updatedAt'.
        connection_id (UUID | Unset): Filter by connection ID
        model_id (UUID | Unset): Filter by model ID. Shared models return connection uploads;
            workbook models return their own uploads.
        search_term (str | Unset): Search term to filter by file name
        type_ (UploadsListType | Unset): Filter by upload type (default: csv) Default: 'csv'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UploadsListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            connection_id=connection_id,
            model_id=model_id,
            search_term=search_term,
            type_=type_,
        )
    ).parsed
