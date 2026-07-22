from http import HTTPStatus
from typing import Any
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_conversations_list_response import AiConversationsListResponse
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/conversations",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiConversationsListResponse | ApiError401 | ApiError403 | None:
    if response.status_code == 200:
        response_200 = AiConversationsListResponse.from_dict(response.json())

        return response_200

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
) -> Response[AiConversationsListResponse | ApiError401 | ApiError403]:
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
    user_id: UUID | Unset = UNSET,
) -> Response[AiConversationsListResponse | ApiError401 | ApiError403]:
    """List AI conversations

     List the user's recent AI conversations, ordered by most-recent activity. Each record includes the
    conversation id (pass it back as `conversationId` on subsequent /api/v1/ai/jobs submissions to
    continue the thread), an optional name, and a one-line summary of the most recent prompt for
    display. Paginated via opaque `pageInfo.nextCursor` — pass it back as `cursor` to fetch the next
    page.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiConversationsListResponse | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
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
    user_id: UUID | Unset = UNSET,
) -> AiConversationsListResponse | ApiError401 | ApiError403 | None:
    """List AI conversations

     List the user's recent AI conversations, ordered by most-recent activity. Each record includes the
    conversation id (pass it back as `conversationId` on subsequent /api/v1/ai/jobs submissions to
    continue the thread), an optional name, and a one-line summary of the most recent prompt for
    display. Paginated via opaque `pageInfo.nextCursor` — pass it back as `cursor` to fetch the next
    page.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiConversationsListResponse | ApiError401 | ApiError403
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    user_id: UUID | Unset = UNSET,
) -> Response[AiConversationsListResponse | ApiError401 | ApiError403]:
    """List AI conversations

     List the user's recent AI conversations, ordered by most-recent activity. Each record includes the
    conversation id (pass it back as `conversationId` on subsequent /api/v1/ai/jobs submissions to
    continue the thread), an optional name, and a one-line summary of the most recent prompt for
    display. Paginated via opaque `pageInfo.nextCursor` — pass it back as `cursor` to fetch the next
    page.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiConversationsListResponse | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    user_id: UUID | Unset = UNSET,
) -> AiConversationsListResponse | ApiError401 | ApiError403 | None:
    """List AI conversations

     List the user's recent AI conversations, ordered by most-recent activity. Each record includes the
    conversation id (pass it back as `conversationId` on subsequent /api/v1/ai/jobs submissions to
    continue the thread), an optional name, and a one-line summary of the most recent prompt for
    display. Paginated via opaque `pageInfo.nextCursor` — pass it back as `cursor` to fetch the next
    page.

    Args:
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiConversationsListResponse | ApiError401 | ApiError403
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            user_id=user_id,
        )
    ).parsed
