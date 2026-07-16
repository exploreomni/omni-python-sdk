from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_list_response import DocumentsListResponse
from ...models.documents_list_sort_direction import DocumentsListSortDirection
from ...models.documents_list_sort_field import DocumentsListSortField
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    creator_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    page_size: int | Unset = 50,
    sort_direction: DocumentsListSortDirection | Unset = "asc",
    sort_field: DocumentsListSortField | Unset = "name",
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_creator_id: str | Unset = UNSET
    if not isinstance(creator_id, Unset):
        json_creator_id = str(creator_id)
    params["creatorId"] = json_creator_id

    params["cursor"] = cursor

    json_folder_id: str | Unset = UNSET
    if not isinstance(folder_id, Unset):
        json_folder_id = str(folder_id)
    params["folderId"] = json_folder_id

    params["include"] = include

    params["labels"] = labels

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    json_sort_field: str | Unset = UNSET
    if not isinstance(sort_field, Unset):
        json_sort_field = sort_field

    params["sortField"] = json_sort_field

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/documents",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsListResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsListResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    creator_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    page_size: int | Unset = 50,
    sort_direction: DocumentsListSortDirection | Unset = "asc",
    sort_field: DocumentsListSortField | Unset = "name",
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsListResponse]:
    """List documents

    Args:
        creator_id (UUID | Unset): Filter by creator membership ID
        cursor (str | Unset): Cursor for pagination
        folder_id (UUID | Unset): Filter by folder ID
        include (str | Unset): Comma-separated list of additional fields to include: _count,
            labels, includeDeleted, onlyFavorites, onlySharedWithMe. onlySharedWithMe requires userId
            or user-scoped key and cannot be combined with onlyFavorites or folderId. Example:
            _count,labels.
        labels (str | Unset): Comma-separated list of label names to filter by Example:
            verified,important.
        page_size (int | Unset): Number of records per page Default: 50.
        sort_direction (DocumentsListSortDirection | Unset): Sort direction Default: 'asc'.
        sort_field (DocumentsListSortField | Unset): Field to sort by Default: 'name'.
        user_id (UUID | Unset): Filter documents visible to this membership ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsListResponse]
    """

    kwargs = _get_kwargs(
        creator_id=creator_id,
        cursor=cursor,
        folder_id=folder_id,
        include=include,
        labels=labels,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    creator_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    page_size: int | Unset = 50,
    sort_direction: DocumentsListSortDirection | Unset = "asc",
    sort_field: DocumentsListSortField | Unset = "name",
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsListResponse | None:
    """List documents

    Args:
        creator_id (UUID | Unset): Filter by creator membership ID
        cursor (str | Unset): Cursor for pagination
        folder_id (UUID | Unset): Filter by folder ID
        include (str | Unset): Comma-separated list of additional fields to include: _count,
            labels, includeDeleted, onlyFavorites, onlySharedWithMe. onlySharedWithMe requires userId
            or user-scoped key and cannot be combined with onlyFavorites or folderId. Example:
            _count,labels.
        labels (str | Unset): Comma-separated list of label names to filter by Example:
            verified,important.
        page_size (int | Unset): Number of records per page Default: 50.
        sort_direction (DocumentsListSortDirection | Unset): Sort direction Default: 'asc'.
        sort_field (DocumentsListSortField | Unset): Field to sort by Default: 'name'.
        user_id (UUID | Unset): Filter documents visible to this membership ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsListResponse
    """

    return sync_detailed(
        client=client,
        creator_id=creator_id,
        cursor=cursor,
        folder_id=folder_id,
        include=include,
        labels=labels,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    creator_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    page_size: int | Unset = 50,
    sort_direction: DocumentsListSortDirection | Unset = "asc",
    sort_field: DocumentsListSortField | Unset = "name",
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsListResponse]:
    """List documents

    Args:
        creator_id (UUID | Unset): Filter by creator membership ID
        cursor (str | Unset): Cursor for pagination
        folder_id (UUID | Unset): Filter by folder ID
        include (str | Unset): Comma-separated list of additional fields to include: _count,
            labels, includeDeleted, onlyFavorites, onlySharedWithMe. onlySharedWithMe requires userId
            or user-scoped key and cannot be combined with onlyFavorites or folderId. Example:
            _count,labels.
        labels (str | Unset): Comma-separated list of label names to filter by Example:
            verified,important.
        page_size (int | Unset): Number of records per page Default: 50.
        sort_direction (DocumentsListSortDirection | Unset): Sort direction Default: 'asc'.
        sort_field (DocumentsListSortField | Unset): Field to sort by Default: 'name'.
        user_id (UUID | Unset): Filter documents visible to this membership ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsListResponse]
    """

    kwargs = _get_kwargs(
        creator_id=creator_id,
        cursor=cursor,
        folder_id=folder_id,
        include=include,
        labels=labels,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    creator_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    folder_id: UUID | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    page_size: int | Unset = 50,
    sort_direction: DocumentsListSortDirection | Unset = "asc",
    sort_field: DocumentsListSortField | Unset = "name",
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsListResponse | None:
    """List documents

    Args:
        creator_id (UUID | Unset): Filter by creator membership ID
        cursor (str | Unset): Cursor for pagination
        folder_id (UUID | Unset): Filter by folder ID
        include (str | Unset): Comma-separated list of additional fields to include: _count,
            labels, includeDeleted, onlyFavorites, onlySharedWithMe. onlySharedWithMe requires userId
            or user-scoped key and cannot be combined with onlyFavorites or folderId. Example:
            _count,labels.
        labels (str | Unset): Comma-separated list of label names to filter by Example:
            verified,important.
        page_size (int | Unset): Number of records per page Default: 50.
        sort_direction (DocumentsListSortDirection | Unset): Sort direction Default: 'asc'.
        sort_field (DocumentsListSortField | Unset): Field to sort by Default: 'name'.
        user_id (UUID | Unset): Filter documents visible to this membership ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            creator_id=creator_id,
            cursor=cursor,
            folder_id=folder_id,
            include=include,
            labels=labels,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            user_id=user_id,
        )
    ).parsed
