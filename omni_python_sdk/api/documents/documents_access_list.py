from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_access_list_access_source import (
    DocumentsAccessListAccessSource,
)
from ...models.documents_access_list_response import DocumentsAccessListResponse
from ...models.documents_access_list_sort_direction import (
    DocumentsAccessListSortDirection,
)
from ...models.documents_access_list_type import DocumentsAccessListType
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsAccessListSortDirection | Unset = "asc",
    sort_field: str | Unset = UNSET,
    access_source: DocumentsAccessListAccessSource | Unset = UNSET,
    type_: DocumentsAccessListType | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    params["sortField"] = sort_field

    json_access_source: str | Unset = UNSET
    if not isinstance(access_source, Unset):
        json_access_source = access_source

    params["accessSource"] = json_access_source

    json_type_: str | Unset = UNSET
    if not isinstance(type_, Unset):
        json_type_ = type_

    params["type"] = json_type_

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/documents/{identifier}/access-list".format(
            identifier=quote(str(identifier), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsAccessListResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsAccessListResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsAccessListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsAccessListSortDirection | Unset = "asc",
    sort_field: str | Unset = UNSET,
    access_source: DocumentsAccessListAccessSource | Unset = UNSET,
    type_: DocumentsAccessListType | Unset = UNSET,
) -> Response[Any | DocumentsAccessListResponse]:
    """List document access principals

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsAccessListSortDirection | Unset): Sort direction (default: asc)
            Default: 'asc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        access_source (DocumentsAccessListAccessSource | Unset): Filter by access source: direct
            or folder
        type_ (DocumentsAccessListType | Unset): Filter by principal type: user or userGroup

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsAccessListResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        access_source=access_source,
        type_=type_,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsAccessListSortDirection | Unset = "asc",
    sort_field: str | Unset = UNSET,
    access_source: DocumentsAccessListAccessSource | Unset = UNSET,
    type_: DocumentsAccessListType | Unset = UNSET,
) -> Any | DocumentsAccessListResponse | None:
    """List document access principals

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsAccessListSortDirection | Unset): Sort direction (default: asc)
            Default: 'asc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        access_source (DocumentsAccessListAccessSource | Unset): Filter by access source: direct
            or folder
        type_ (DocumentsAccessListType | Unset): Filter by principal type: user or userGroup

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsAccessListResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        access_source=access_source,
        type_=type_,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsAccessListSortDirection | Unset = "asc",
    sort_field: str | Unset = UNSET,
    access_source: DocumentsAccessListAccessSource | Unset = UNSET,
    type_: DocumentsAccessListType | Unset = UNSET,
) -> Response[Any | DocumentsAccessListResponse]:
    """List document access principals

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsAccessListSortDirection | Unset): Sort direction (default: asc)
            Default: 'asc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        access_source (DocumentsAccessListAccessSource | Unset): Filter by access source: direct
            or folder
        type_ (DocumentsAccessListType | Unset): Filter by principal type: user or userGroup

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsAccessListResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        access_source=access_source,
        type_=type_,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsAccessListSortDirection | Unset = "asc",
    sort_field: str | Unset = UNSET,
    access_source: DocumentsAccessListAccessSource | Unset = UNSET,
    type_: DocumentsAccessListType | Unset = UNSET,
) -> Any | DocumentsAccessListResponse | None:
    """List document access principals

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsAccessListSortDirection | Unset): Sort direction (default: asc)
            Default: 'asc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        access_source (DocumentsAccessListAccessSource | Unset): Filter by access source: direct
            or folder
        type_ (DocumentsAccessListType | Unset): Filter by principal type: user or userGroup

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsAccessListResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            access_source=access_source,
            type_=type_,
        )
    ).parsed
