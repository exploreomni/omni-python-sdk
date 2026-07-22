from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.content_list_response import ContentListResponse
from ...models.content_list_scope import ContentListScope
from ...models.content_list_sort_direction import ContentListSortDirection
from ...models.content_list_sort_field import ContentListSortField
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    creator_id: UUID | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: ContentListScope | Unset = UNSET,
    sort_direction: ContentListSortDirection | Unset = UNSET,
    sort_field: ContentListSortField | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_creator_id: str | Unset = UNSET
    if not isinstance(creator_id, Unset):
        json_creator_id = str(creator_id)
    params["creatorId"] = json_creator_id

    json_folder_id: str | Unset = UNSET
    if not isinstance(folder_id, Unset):
        json_folder_id = str(folder_id)
    params["folderId"] = json_folder_id

    params["include"] = include

    params["path"] = path

    json_scope: str | Unset = UNSET
    if not isinstance(scope, Unset):
        json_scope = scope

    params["scope"] = json_scope

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
        "url": "/api/v1/content",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ContentListResponse | None:
    if response.status_code == 200:
        response_200 = ContentListResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ContentListResponse]:
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
    creator_id: UUID | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: ContentListScope | Unset = UNSET,
    sort_direction: ContentListSortDirection | Unset = UNSET,
    sort_field: ContentListSortField | Unset = UNSET,
) -> Response[Any | ContentListResponse]:
    """List content

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        creator_id (UUID | Unset): Filter by creator user ID
        folder_id (UUID | Unset): Filter by folder ID (cannot be used with path)
        include (str | Unset): Comma-separated list of fields to include (e.g., _count,labels)
        path (str | Unset): Filter by folder path (cannot be used with folderId) Example:
            /reports/sales.
        scope (ContentListScope | Unset): Filter by share scope Example: organization.
        sort_direction (ContentListSortDirection | Unset): Sort direction Example: asc.
        sort_field (ContentListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ContentListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        creator_id=creator_id,
        folder_id=folder_id,
        include=include,
        path=path,
        scope=scope,
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
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    creator_id: UUID | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: ContentListScope | Unset = UNSET,
    sort_direction: ContentListSortDirection | Unset = UNSET,
    sort_field: ContentListSortField | Unset = UNSET,
) -> Any | ContentListResponse | None:
    """List content

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        creator_id (UUID | Unset): Filter by creator user ID
        folder_id (UUID | Unset): Filter by folder ID (cannot be used with path)
        include (str | Unset): Comma-separated list of fields to include (e.g., _count,labels)
        path (str | Unset): Filter by folder path (cannot be used with folderId) Example:
            /reports/sales.
        scope (ContentListScope | Unset): Filter by share scope Example: organization.
        sort_direction (ContentListSortDirection | Unset): Sort direction Example: asc.
        sort_field (ContentListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ContentListResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        creator_id=creator_id,
        folder_id=folder_id,
        include=include,
        path=path,
        scope=scope,
        sort_direction=sort_direction,
        sort_field=sort_field,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    creator_id: UUID | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: ContentListScope | Unset = UNSET,
    sort_direction: ContentListSortDirection | Unset = UNSET,
    sort_field: ContentListSortField | Unset = UNSET,
) -> Response[Any | ContentListResponse]:
    """List content

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        creator_id (UUID | Unset): Filter by creator user ID
        folder_id (UUID | Unset): Filter by folder ID (cannot be used with path)
        include (str | Unset): Comma-separated list of fields to include (e.g., _count,labels)
        path (str | Unset): Filter by folder path (cannot be used with folderId) Example:
            /reports/sales.
        scope (ContentListScope | Unset): Filter by share scope Example: organization.
        sort_direction (ContentListSortDirection | Unset): Sort direction Example: asc.
        sort_field (ContentListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ContentListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        creator_id=creator_id,
        folder_id=folder_id,
        include=include,
        path=path,
        scope=scope,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    creator_id: UUID | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: ContentListScope | Unset = UNSET,
    sort_direction: ContentListSortDirection | Unset = UNSET,
    sort_field: ContentListSortField | Unset = UNSET,
) -> Any | ContentListResponse | None:
    """List content

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        creator_id (UUID | Unset): Filter by creator user ID
        folder_id (UUID | Unset): Filter by folder ID (cannot be used with path)
        include (str | Unset): Comma-separated list of fields to include (e.g., _count,labels)
        path (str | Unset): Filter by folder path (cannot be used with folderId) Example:
            /reports/sales.
        scope (ContentListScope | Unset): Filter by share scope Example: organization.
        sort_direction (ContentListSortDirection | Unset): Sort direction Example: asc.
        sort_field (ContentListSortField | Unset): Field to sort by Example: name.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ContentListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            creator_id=creator_id,
            folder_id=folder_id,
            include=include,
            path=path,
            scope=scope,
            sort_direction=sort_direction,
            sort_field=sort_field,
        )
    ).parsed
