from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.users_list_email_only_response import UsersListEmailOnlyResponse
from ...models.users_list_email_only_sort_direction import (
    UsersListEmailOnlySortDirection,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    email: str | Unset = UNSET,
    page_size: float | Unset = 20.0,
    sort_direction: UsersListEmailOnlySortDirection | Unset = "desc",
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["email"] = email

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/users/email-only",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | UsersListEmailOnlyResponse | None:
    if response.status_code == 200:
        response_200 = UsersListEmailOnlyResponse.from_dict(response.json())

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
) -> Response[Any | UsersListEmailOnlyResponse]:
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
    email: str | Unset = UNSET,
    page_size: float | Unset = 20.0,
    sort_direction: UsersListEmailOnlySortDirection | Unset = "desc",
) -> Response[Any | UsersListEmailOnlyResponse]:
    """List email-only users

    Args:
        cursor (str | Unset): Cursor for pagination
        email (str | Unset): Filter by email address Example: user@example.com.
        page_size (float | Unset): Number of results per page (max 20) Default: 20.0. Example: 20.
        sort_direction (UsersListEmailOnlySortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UsersListEmailOnlyResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        email=email,
        page_size=page_size,
        sort_direction=sort_direction,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    email: str | Unset = UNSET,
    page_size: float | Unset = 20.0,
    sort_direction: UsersListEmailOnlySortDirection | Unset = "desc",
) -> Any | UsersListEmailOnlyResponse | None:
    """List email-only users

    Args:
        cursor (str | Unset): Cursor for pagination
        email (str | Unset): Filter by email address Example: user@example.com.
        page_size (float | Unset): Number of results per page (max 20) Default: 20.0. Example: 20.
        sort_direction (UsersListEmailOnlySortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UsersListEmailOnlyResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        email=email,
        page_size=page_size,
        sort_direction=sort_direction,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    email: str | Unset = UNSET,
    page_size: float | Unset = 20.0,
    sort_direction: UsersListEmailOnlySortDirection | Unset = "desc",
) -> Response[Any | UsersListEmailOnlyResponse]:
    """List email-only users

    Args:
        cursor (str | Unset): Cursor for pagination
        email (str | Unset): Filter by email address Example: user@example.com.
        page_size (float | Unset): Number of results per page (max 20) Default: 20.0. Example: 20.
        sort_direction (UsersListEmailOnlySortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | UsersListEmailOnlyResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        email=email,
        page_size=page_size,
        sort_direction=sort_direction,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    email: str | Unset = UNSET,
    page_size: float | Unset = 20.0,
    sort_direction: UsersListEmailOnlySortDirection | Unset = "desc",
) -> Any | UsersListEmailOnlyResponse | None:
    """List email-only users

    Args:
        cursor (str | Unset): Cursor for pagination
        email (str | Unset): Filter by email address Example: user@example.com.
        page_size (float | Unset): Number of results per page (max 20) Default: 20.0. Example: 20.
        sort_direction (UsersListEmailOnlySortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | UsersListEmailOnlyResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            email=email,
            page_size=page_size,
            sort_direction=sort_direction,
        )
    ).parsed
