from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.scim_users_list_response import ScimUsersListResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    count: str | Unset = "100",
    filter_: str | Unset = UNSET,
    start_index: str | Unset = "1",
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["count"] = count

    params["filter"] = filter_

    params["startIndex"] = start_index

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/scim/v2/Users",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ScimUsersListResponse | None:
    if response.status_code == 200:
        response_200 = ScimUsersListResponse.from_dict(response.json())

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
) -> Response[Any | ScimUsersListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    count: str | Unset = "100",
    filter_: str | Unset = UNSET,
    start_index: str | Unset = "1",
) -> Response[Any | ScimUsersListResponse]:
    """List SCIM users

    Args:
        count (str | Unset): Maximum number of results to return Default: '100'. Example: 100.
        filter_ (str | Unset): SCIM filter expression Example: userName eq "user@example.com".
        start_index (str | Unset): Index of the first result to return (1-based) Default: '1'.
            Example: 1.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimUsersListResponse]
    """

    kwargs = _get_kwargs(
        count=count,
        filter_=filter_,
        start_index=start_index,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    count: str | Unset = "100",
    filter_: str | Unset = UNSET,
    start_index: str | Unset = "1",
) -> Any | ScimUsersListResponse | None:
    """List SCIM users

    Args:
        count (str | Unset): Maximum number of results to return Default: '100'. Example: 100.
        filter_ (str | Unset): SCIM filter expression Example: userName eq "user@example.com".
        start_index (str | Unset): Index of the first result to return (1-based) Default: '1'.
            Example: 1.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScimUsersListResponse
    """

    return sync_detailed(
        client=client,
        count=count,
        filter_=filter_,
        start_index=start_index,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    count: str | Unset = "100",
    filter_: str | Unset = UNSET,
    start_index: str | Unset = "1",
) -> Response[Any | ScimUsersListResponse]:
    """List SCIM users

    Args:
        count (str | Unset): Maximum number of results to return Default: '100'. Example: 100.
        filter_ (str | Unset): SCIM filter expression Example: userName eq "user@example.com".
        start_index (str | Unset): Index of the first result to return (1-based) Default: '1'.
            Example: 1.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimUsersListResponse]
    """

    kwargs = _get_kwargs(
        count=count,
        filter_=filter_,
        start_index=start_index,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    count: str | Unset = "100",
    filter_: str | Unset = UNSET,
    start_index: str | Unset = "1",
) -> Any | ScimUsersListResponse | None:
    """List SCIM users

    Args:
        count (str | Unset): Maximum number of results to return Default: '100'. Example: 100.
        filter_ (str | Unset): SCIM filter expression Example: userName eq "user@example.com".
        start_index (str | Unset): Index of the first result to return (1-based) Default: '1'.
            Example: 1.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScimUsersListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            count=count,
            filter_=filter_,
            start_index=start_index,
        )
    ).parsed
