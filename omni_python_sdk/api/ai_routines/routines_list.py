from http import HTTPStatus
from typing import Any
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.routines_list_response import RoutinesListResponse
from ...models.routines_list_sort_direction import RoutinesListSortDirection
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: RoutinesListSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    params["sortField"] = sort_field

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/routines",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse | None:
    if response.status_code == 200:
        response_200 = RoutinesListResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ApiError404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse]:
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
    sort_direction: RoutinesListSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse]:
    """List routines

     List routines for the calling user, newest first. Includes routines paused by the owner or disabled
    by Omni, but excludes deleted routines. Use `pageInfo.nextCursor` from one response as the `cursor`
    query parameter on the next request. Organization API keys can pass `?userId=<membershipId>` to list
    routines for a specific organization member.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (RoutinesListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
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
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: RoutinesListSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse | None:
    """List routines

     List routines for the calling user, newest first. Includes routines paused by the owner or disabled
    by Omni, but excludes deleted routines. Use `pageInfo.nextCursor` from one response as the `cursor`
    query parameter on the next request. Organization API keys can pass `?userId=<membershipId>` to list
    routines for a specific organization member.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (RoutinesListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: RoutinesListSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse]:
    """List routines

     List routines for the calling user, newest first. Includes routines paused by the owner or disabled
    by Omni, but excludes deleted routines. Use `pageInfo.nextCursor` from one response as the `cursor`
    query parameter on the next request. Organization API keys can pass `?userId=<membershipId>` to list
    routines for a specific organization member.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (RoutinesListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
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
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: RoutinesListSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse | None:
    """List routines

     List routines for the calling user, newest first. Includes routines paused by the owner or disabled
    by Omni, but excludes deleted routines. Use `pageInfo.nextCursor` from one response as the `cursor`
    query parameter on the next request. Organization API keys can pass `?userId=<membershipId>` to list
    routines for a specific organization member.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (RoutinesListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutinesListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            user_id=user_id,
        )
    ).parsed
