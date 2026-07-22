from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.model_suggestions_list_response import ModelSuggestionsListResponse
from ...models.model_suggestions_list_status import ModelSuggestionsListStatus
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    status: ModelSuggestionsListStatus | Unset = "active",
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_cursor: str | Unset = UNSET
    if not isinstance(cursor, Unset):
        json_cursor = str(cursor)
    params["cursor"] = json_cursor

    params["pageSize"] = page_size

    json_status: str | Unset = UNSET
    if not isinstance(status, Unset):
        json_status = status

    params["status"] = json_status

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/suggestions".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelSuggestionsListResponse | None:
    if response.status_code == 200:
        response_200 = ModelSuggestionsListResponse.from_dict(response.json())

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
) -> Response[Any | ModelSuggestionsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    status: ModelSuggestionsListStatus | Unset = "active",
) -> Response[Any | ModelSuggestionsListResponse]:
    """List model suggestions

     Lists AI-generated model suggestions for a shared model, filtered by dismissal status. Requires
    organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        cursor (UUID | Unset): Cursor for pagination: the `nextCursor` from the previous response
            (the last suggestion id).
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        status (ModelSuggestionsListStatus | Unset): Which suggestions to return: `active`
            (default, not dismissed), `ignored` (dismissed only), or `all`. Default: 'active'.
            Example: active.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelSuggestionsListResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        cursor=cursor,
        page_size=page_size,
        status=status,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    status: ModelSuggestionsListStatus | Unset = "active",
) -> Any | ModelSuggestionsListResponse | None:
    """List model suggestions

     Lists AI-generated model suggestions for a shared model, filtered by dismissal status. Requires
    organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        cursor (UUID | Unset): Cursor for pagination: the `nextCursor` from the previous response
            (the last suggestion id).
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        status (ModelSuggestionsListStatus | Unset): Which suggestions to return: `active`
            (default, not dismissed), `ignored` (dismissed only), or `all`. Default: 'active'.
            Example: active.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelSuggestionsListResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        cursor=cursor,
        page_size=page_size,
        status=status,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    status: ModelSuggestionsListStatus | Unset = "active",
) -> Response[Any | ModelSuggestionsListResponse]:
    """List model suggestions

     Lists AI-generated model suggestions for a shared model, filtered by dismissal status. Requires
    organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        cursor (UUID | Unset): Cursor for pagination: the `nextCursor` from the previous response
            (the last suggestion id).
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        status (ModelSuggestionsListStatus | Unset): Which suggestions to return: `active`
            (default, not dismissed), `ignored` (dismissed only), or `all`. Default: 'active'.
            Example: active.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelSuggestionsListResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        cursor=cursor,
        page_size=page_size,
        status=status,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: UUID | Unset = UNSET,
    page_size: int | Unset = 20,
    status: ModelSuggestionsListStatus | Unset = "active",
) -> Any | ModelSuggestionsListResponse | None:
    """List model suggestions

     Lists AI-generated model suggestions for a shared model, filtered by dismissal status. Requires
    organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        cursor (UUID | Unset): Cursor for pagination: the `nextCursor` from the previous response
            (the last suggestion id).
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        status (ModelSuggestionsListStatus | Unset): Which suggestions to return: `active`
            (default, not dismissed), `ignored` (dismissed only), or `all`. Default: 'active'.
            Example: active.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelSuggestionsListResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            cursor=cursor,
            page_size=page_size,
            status=status,
        )
    ).parsed
