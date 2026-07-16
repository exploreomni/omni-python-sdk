from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_credit_controls_users_list_response import AiCreditControlsUsersListResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/credit-controls/users",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403 | None:
    if response.status_code == 200:
        response_200 = AiCreditControlsUsersListResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403]:
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
) -> Response[AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403]:
    """List individual users' AI credit limits

     List the organization's active individual user AI credit limits, ordered by userId ascending. Only
    users with an individual limit appear — everyone else follows the org default. A `null` creditLimit
    is an explicit unlimited override, distinct from following the default. Paginated via opaque
    cursors: pass `pageInfo.nextCursor` from one response as the `cursor` query parameter on the next
    request. Requires the same manage-user-attributes permission as the PATCH.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
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
) -> AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403 | None:
    """List individual users' AI credit limits

     List the organization's active individual user AI credit limits, ordered by userId ascending. Only
    users with an individual limit appear — everyone else follows the org default. A `null` creditLimit
    is an explicit unlimited override, distinct from following the default. Paginated via opaque
    cursors: pass `pageInfo.nextCursor` from one response as the `cursor` query parameter on the next
    request. Requires the same manage-user-attributes permission as the PATCH.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
) -> Response[AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403]:
    """List individual users' AI credit limits

     List the organization's active individual user AI credit limits, ordered by userId ascending. Only
    users with an individual limit appear — everyone else follows the org default. A `null` creditLimit
    is an explicit unlimited override, distinct from following the default. Paginated via opaque
    cursors: pass `pageInfo.nextCursor` from one response as the `cursor` query parameter on the next
    request. Requires the same manage-user-attributes permission as the PATCH.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
) -> AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403 | None:
    """List individual users' AI credit limits

     List the organization's active individual user AI credit limits, ordered by userId ascending. Only
    users with an individual limit appear — everyone else follows the org default. A `null` creditLimit
    is an explicit unlimited override, distinct from following the default. Paginated via opaque
    cursors: pass `pageInfo.nextCursor` from one response as the `cursor` query parameter on the next
    request. Requires the same manage-user-attributes permission as the PATCH.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditControlsUsersListResponse | ApiError400 | ApiError401 | ApiError403
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
        )
    ).parsed
