from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_key_list_response import ApiKeyListResponse
from ...models.api_keys_list_sort_direction import ApiKeysListSortDirection
from ...models.api_keys_list_sort_field import ApiKeysListSortField
from ...models.api_keys_list_type import ApiKeysListType
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ApiKeysListSortDirection | Unset = "desc",
    sort_field: ApiKeysListSortField | Unset = "createdAt",
    type_: ApiKeysListType | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_cursor: str | Unset = UNSET
    if not isinstance(cursor, Unset):
        json_cursor = str(cursor)
    params["cursor"] = json_cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    json_sort_field: str | Unset = UNSET
    if not isinstance(sort_field, Unset):
        json_sort_field = sort_field

    params["sortField"] = json_sort_field

    json_type_: str | Unset = UNSET
    if not isinstance(type_, Unset):
        json_type_ = type_

    params["type"] = json_type_

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/api-keys",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ApiKeyListResponse | None:
    if response.status_code == 200:
        response_200 = ApiKeyListResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | ApiKeyListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ApiKeysListSortDirection | Unset = "desc",
    sort_field: ApiKeysListSortField | Unset = "createdAt",
    type_: ApiKeysListType | Unset = UNSET,
) -> Response[Any | ApiKeyListResponse]:
    """List API tokens

     Returns all API tokens in the organization, including organization-level keys, personal access
    tokens, and MCP OAuth grants. Secrets are never returned. Requires organization admin permissions.

    Args:
        cursor (UUID | Unset): Cursor from the previous response (token UUID)
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ApiKeysListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (ApiKeysListSortField | Unset):  Default: 'createdAt'.
        type_ (ApiKeysListType | Unset): Filter by API token type. When omitted, all types are
            returned. Example: personal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ApiKeyListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        type_=type_,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ApiKeysListSortDirection | Unset = "desc",
    sort_field: ApiKeysListSortField | Unset = "createdAt",
    type_: ApiKeysListType | Unset = UNSET,
) -> Any | ApiKeyListResponse | None:
    """List API tokens

     Returns all API tokens in the organization, including organization-level keys, personal access
    tokens, and MCP OAuth grants. Secrets are never returned. Requires organization admin permissions.

    Args:
        cursor (UUID | Unset): Cursor from the previous response (token UUID)
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ApiKeysListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (ApiKeysListSortField | Unset):  Default: 'createdAt'.
        type_ (ApiKeysListType | Unset): Filter by API token type. When omitted, all types are
            returned. Example: personal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ApiKeyListResponse
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        type_=type_,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ApiKeysListSortDirection | Unset = "desc",
    sort_field: ApiKeysListSortField | Unset = "createdAt",
    type_: ApiKeysListType | Unset = UNSET,
) -> Response[Any | ApiKeyListResponse]:
    """List API tokens

     Returns all API tokens in the organization, including organization-level keys, personal access
    tokens, and MCP OAuth grants. Secrets are never returned. Requires organization admin permissions.

    Args:
        cursor (UUID | Unset): Cursor from the previous response (token UUID)
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ApiKeysListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (ApiKeysListSortField | Unset):  Default: 'createdAt'.
        type_ (ApiKeysListType | Unset): Filter by API token type. When omitted, all types are
            returned. Example: personal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ApiKeyListResponse]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        type_=type_,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ApiKeysListSortDirection | Unset = "desc",
    sort_field: ApiKeysListSortField | Unset = "createdAt",
    type_: ApiKeysListType | Unset = UNSET,
) -> Any | ApiKeyListResponse | None:
    """List API tokens

     Returns all API tokens in the organization, including organization-level keys, personal access
    tokens, and MCP OAuth grants. Secrets are never returned. Requires organization admin permissions.

    Args:
        cursor (UUID | Unset): Cursor from the previous response (token UUID)
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ApiKeysListSortDirection | Unset): Sort direction for results Default:
            'desc'. Example: desc.
        sort_field (ApiKeysListSortField | Unset):  Default: 'createdAt'.
        type_ (ApiKeysListType | Unset): Filter by API token type. When omitted, all types are
            returned. Example: personal.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ApiKeyListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            type_=type_,
        )
    ).parsed
