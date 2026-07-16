from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.folders_list_response import FoldersListResponse
from ...models.folders_list_scope import FoldersListScope
from ...models.folders_list_sort_direction import FoldersListSortDirection
from ...models.folders_list_sort_field import FoldersListSortField
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    page_size: float | None | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: FoldersListScope | Unset = UNSET,
    sort_direction: FoldersListSortDirection | Unset = UNSET,
    sort_field: FoldersListSortField | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["include"] = include

    params["labels"] = labels

    json_owner_id: str | Unset = UNSET
    if not isinstance(owner_id, Unset):
        json_owner_id = str(owner_id)
    params["ownerId"] = json_owner_id

    json_page_size: float | None | Unset
    if isinstance(page_size, Unset):
        json_page_size = UNSET
    else:
        json_page_size = page_size
    params["pageSize"] = json_page_size

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

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/folders",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | FoldersListResponse | None:
    if response.status_code == 200:
        response_200 = FoldersListResponse.from_dict(response.json())

        return response_200

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
) -> Response[Any | FoldersListResponse]:
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
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    page_size: float | None | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: FoldersListScope | Unset = UNSET,
    sort_direction: FoldersListSortDirection | Unset = UNSET,
    sort_field: FoldersListSortField | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | FoldersListResponse]:
    """List folders

    Args:
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (_count, labels,
            onlySharedWithMe). onlySharedWithMe returns only folders shared with the user, cannot be
            combined with ownerId or path, and when used with org-scoped API keys requires the userId
            query parameter. For user-scoped keys (PAT), userId is auto-inferred from the token.
            Example: _count,labels.
        labels (str | Unset): Comma-separated list of labels to filter by
        owner_id (UUID | Unset): Filter by owner user ID
        page_size (float | None | Unset): Number of results per page Example: 20.
        path (str | Unset): Filter by exact path
        scope (FoldersListScope | Unset): Filter by share scope
        sort_direction (FoldersListSortDirection | Unset): Sort direction
        sort_field (FoldersListSortField | Unset): Field to sort by
        user_id (UUID | Unset): User membership ID. Only used with onlySharedWithMe include field.
            Required for org-scoped API keys; auto-inferred from the token for user-scoped keys (PAT).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        include=include,
        labels=labels,
        owner_id=owner_id,
        page_size=page_size,
        path=path,
        scope=scope,
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
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    page_size: float | None | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: FoldersListScope | Unset = UNSET,
    sort_direction: FoldersListSortDirection | Unset = UNSET,
    sort_field: FoldersListSortField | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | FoldersListResponse | None:
    """List folders

    Args:
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (_count, labels,
            onlySharedWithMe). onlySharedWithMe returns only folders shared with the user, cannot be
            combined with ownerId or path, and when used with org-scoped API keys requires the userId
            query parameter. For user-scoped keys (PAT), userId is auto-inferred from the token.
            Example: _count,labels.
        labels (str | Unset): Comma-separated list of labels to filter by
        owner_id (UUID | Unset): Filter by owner user ID
        page_size (float | None | Unset): Number of results per page Example: 20.
        path (str | Unset): Filter by exact path
        scope (FoldersListScope | Unset): Filter by share scope
        sort_direction (FoldersListSortDirection | Unset): Sort direction
        sort_field (FoldersListSortField | Unset): Field to sort by
        user_id (UUID | Unset): User membership ID. Only used with onlySharedWithMe include field.
            Required for org-scoped API keys; auto-inferred from the token for user-scoped keys (PAT).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersListResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        include=include,
        labels=labels,
        owner_id=owner_id,
        page_size=page_size,
        path=path,
        scope=scope,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    page_size: float | None | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: FoldersListScope | Unset = UNSET,
    sort_direction: FoldersListSortDirection | Unset = UNSET,
    sort_field: FoldersListSortField | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | FoldersListResponse]:
    """List folders

    Args:
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (_count, labels,
            onlySharedWithMe). onlySharedWithMe returns only folders shared with the user, cannot be
            combined with ownerId or path, and when used with org-scoped API keys requires the userId
            query parameter. For user-scoped keys (PAT), userId is auto-inferred from the token.
            Example: _count,labels.
        labels (str | Unset): Comma-separated list of labels to filter by
        owner_id (UUID | Unset): Filter by owner user ID
        page_size (float | None | Unset): Number of results per page Example: 20.
        path (str | Unset): Filter by exact path
        scope (FoldersListScope | Unset): Filter by share scope
        sort_direction (FoldersListSortDirection | Unset): Sort direction
        sort_field (FoldersListSortField | Unset): Field to sort by
        user_id (UUID | Unset): User membership ID. Only used with onlySharedWithMe include field.
            Required for org-scoped API keys; auto-inferred from the token for user-scoped keys (PAT).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | FoldersListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        include=include,
        labels=labels,
        owner_id=owner_id,
        page_size=page_size,
        path=path,
        scope=scope,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    labels: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    page_size: float | None | Unset = UNSET,
    path: str | Unset = UNSET,
    scope: FoldersListScope | Unset = UNSET,
    sort_direction: FoldersListSortDirection | Unset = UNSET,
    sort_field: FoldersListSortField | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | FoldersListResponse | None:
    """List folders

    Args:
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (_count, labels,
            onlySharedWithMe). onlySharedWithMe returns only folders shared with the user, cannot be
            combined with ownerId or path, and when used with org-scoped API keys requires the userId
            query parameter. For user-scoped keys (PAT), userId is auto-inferred from the token.
            Example: _count,labels.
        labels (str | Unset): Comma-separated list of labels to filter by
        owner_id (UUID | Unset): Filter by owner user ID
        page_size (float | None | Unset): Number of results per page Example: 20.
        path (str | Unset): Filter by exact path
        scope (FoldersListScope | Unset): Filter by share scope
        sort_direction (FoldersListSortDirection | Unset): Sort direction
        sort_field (FoldersListSortField | Unset): Field to sort by
        user_id (UUID | Unset): User membership ID. Only used with onlySharedWithMe include field.
            Required for org-scoped API keys; auto-inferred from the token for user-scoped keys (PAT).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | FoldersListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            include=include,
            labels=labels,
            owner_id=owner_id,
            page_size=page_size,
            path=path,
            scope=scope,
            sort_direction=sort_direction,
            sort_field=sort_field,
            user_id=user_id,
        )
    ).parsed
