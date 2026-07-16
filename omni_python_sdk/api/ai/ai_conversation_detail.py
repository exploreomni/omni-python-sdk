from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_conversation_detail_response import AiConversationDetailResponse
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...types import Response


def _get_kwargs(
    conversation_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/conversations/{conversation_id}".format(
            conversation_id=quote(str(conversation_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404 | None:
    if response.status_code == 200:
        response_200 = AiConversationDetailResponse.from_dict(response.json())

        return response_200

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
) -> Response[AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    conversation_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404]:
    """Get AI conversation with messages

     Return a conversation with its full message history (alternating user / assistant turns). Used by
    clients (iOS app, embed widgets) to restore a prior conversation in their UI.

    Args:
        conversation_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        conversation_id=conversation_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    conversation_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404 | None:
    """Get AI conversation with messages

     Return a conversation with its full message history (alternating user / assistant turns). Used by
    clients (iOS app, embed widgets) to restore a prior conversation in their UI.

    Args:
        conversation_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404
    """

    return sync_detailed(
        conversation_id=conversation_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    conversation_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404]:
    """Get AI conversation with messages

     Return a conversation with its full message history (alternating user / assistant turns). Used by
    clients (iOS app, embed widgets) to restore a prior conversation in their UI.

    Args:
        conversation_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        conversation_id=conversation_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    conversation_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404 | None:
    """Get AI conversation with messages

     Return a conversation with its full message history (alternating user / assistant turns). Used by
    clients (iOS app, embed widgets) to restore a prior conversation in their UI.

    Args:
        conversation_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiConversationDetailResponse | ApiError401 | ApiError403 | ApiError404
    """

    return (
        await asyncio_detailed(
            conversation_id=conversation_id,
            client=client,
        )
    ).parsed
