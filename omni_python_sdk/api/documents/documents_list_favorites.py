from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_list_favorites_response import DocumentsListFavoritesResponse
from ...models.documents_list_favorites_sort_direction import (
    DocumentsListFavoritesSortDirection,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsListFavoritesSortDirection | Unset = "asc",
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/documents/{identifier}/favorites".format(
            identifier=quote(str(identifier), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsListFavoritesResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsListFavoritesResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsListFavoritesResponse]:
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
    sort_direction: DocumentsListFavoritesSortDirection | Unset = "asc",
) -> Response[Any | DocumentsListFavoritesResponse]:
    """List users who favorited the document

     Lists users who have favorited the document, paginated and sorted by favoritedAt. Document-centric
    counterpart to GET /api/v1/documents?include=onlyFavorites: useful for migration scripts that need
    to preserve favorites when replacing documents, without iterating every user in the organization.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsListFavoritesSortDirection | Unset): Sort direction by
            favoritedAt (default: asc — oldest first) Default: 'asc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsListFavoritesResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
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
    sort_direction: DocumentsListFavoritesSortDirection | Unset = "asc",
) -> Any | DocumentsListFavoritesResponse | None:
    """List users who favorited the document

     Lists users who have favorited the document, paginated and sorted by favoritedAt. Document-centric
    counterpart to GET /api/v1/documents?include=onlyFavorites: useful for migration scripts that need
    to preserve favorites when replacing documents, without iterating every user in the organization.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsListFavoritesSortDirection | Unset): Sort direction by
            favoritedAt (default: asc — oldest first) Default: 'asc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsListFavoritesResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsListFavoritesSortDirection | Unset = "asc",
) -> Response[Any | DocumentsListFavoritesResponse]:
    """List users who favorited the document

     Lists users who have favorited the document, paginated and sorted by favoritedAt. Document-centric
    counterpart to GET /api/v1/documents?include=onlyFavorites: useful for migration scripts that need
    to preserve favorites when replacing documents, without iterating every user in the organization.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsListFavoritesSortDirection | Unset): Sort direction by
            favoritedAt (default: asc — oldest first) Default: 'asc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsListFavoritesResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: DocumentsListFavoritesSortDirection | Unset = "asc",
) -> Any | DocumentsListFavoritesResponse | None:
    """List users who favorited the document

     Lists users who have favorited the document, paginated and sorted by favoritedAt. Document-centric
    counterpart to GET /api/v1/documents?include=onlyFavorites: useful for migration scripts that need
    to preserve favorites when replacing documents, without iterating every user in the organization.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (DocumentsListFavoritesSortDirection | Unset): Sort direction by
            favoritedAt (default: asc — oldest first) Default: 'asc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsListFavoritesResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
        )
    ).parsed
