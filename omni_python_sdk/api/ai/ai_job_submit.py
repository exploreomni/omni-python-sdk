from http import HTTPStatus
from typing import Any
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_job_submit_body import AiJobSubmitBody
from ...models.ai_job_submit_response import AiJobSubmitResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.api_error_409 import ApiError409
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: AiJobSubmitBody,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/jobs",
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    if response.status_code == 201:
        response_201 = AiJobSubmitResponse.from_dict(response.json())

        return response_201

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

    if response.status_code == 409:
        response_409 = ApiError409.from_dict(response.json())

        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiJobSubmitBody,
    user_id: UUID | Unset = UNSET,
) -> Response[AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
    """Submit an AI job

     Submit a new AI job for asynchronous execution. The AI will analyze the prompt, generate and execute
    queries against the specified model, and produce a summarized answer. Jobs are processed by a
    background worker and typically complete within 15–60 seconds. Use GET /api/v1/ai/jobs/{jobId} to
    poll for status, or configure a webhookUrl to receive a notification when the job completes.
    Optionally continue an existing conversation by providing a conversationId. The effective user's
    per-connector AI toggles (set in the chat + menu) govern which integration tools the agent may use.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (AiJobSubmitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]
    """

    kwargs = _get_kwargs(
        body=body,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: AiJobSubmitBody,
    user_id: UUID | Unset = UNSET,
) -> AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    """Submit an AI job

     Submit a new AI job for asynchronous execution. The AI will analyze the prompt, generate and execute
    queries against the specified model, and produce a summarized answer. Jobs are processed by a
    background worker and typically complete within 15–60 seconds. Use GET /api/v1/ai/jobs/{jobId} to
    poll for status, or configure a webhookUrl to receive a notification when the job completes.
    Optionally continue an existing conversation by providing a conversationId. The effective user's
    per-connector AI toggles (set in the chat + menu) govern which integration tools the agent may use.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (AiJobSubmitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409
    """

    return sync_detailed(
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiJobSubmitBody,
    user_id: UUID | Unset = UNSET,
) -> Response[AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
    """Submit an AI job

     Submit a new AI job for asynchronous execution. The AI will analyze the prompt, generate and execute
    queries against the specified model, and produce a summarized answer. Jobs are processed by a
    background worker and typically complete within 15–60 seconds. Use GET /api/v1/ai/jobs/{jobId} to
    poll for status, or configure a webhookUrl to receive a notification when the job completes.
    Optionally continue an existing conversation by providing a conversationId. The effective user's
    per-connector AI toggles (set in the chat + menu) govern which integration tools the agent may use.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (AiJobSubmitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]
    """

    kwargs = _get_kwargs(
        body=body,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiJobSubmitBody,
    user_id: UUID | Unset = UNSET,
) -> AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    """Submit an AI job

     Submit a new AI job for asynchronous execution. The AI will analyze the prompt, generate and execute
    queries against the specified model, and produce a summarized answer. Jobs are processed by a
    background worker and typically complete within 15–60 seconds. Use GET /api/v1/ai/jobs/{jobId} to
    poll for status, or configure a webhookUrl to receive a notification when the job completes.
    Optionally continue an existing conversation by providing a conversationId. The effective user's
    per-connector AI toggles (set in the chat + menu) govern which integration tools the agent may use.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (AiJobSubmitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobSubmitResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed
