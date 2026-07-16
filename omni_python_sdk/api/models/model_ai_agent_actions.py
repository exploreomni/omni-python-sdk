from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_agent_actions_response import AiAgentActionsResponse
from ...types import Response


def _get_kwargs(
    model_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/ai-agent-actions".format(
            model_id=quote(str(model_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiAgentActionsResponse | Any | None:
    if response.status_code == 200:
        response_200 = AiAgentActionsResponse.from_dict(response.json())

        return response_200

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
) -> Response[AiAgentActionsResponse | Any]:
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
) -> Response[AiAgentActionsResponse | Any]:
    """Get model AI agent actions

     Returns the AI agent actions configured for this model — a unified list of sample queries and skills
    suitable for surfacing as suggested prompts above an AI prompt input. Sample queries come from both
    `model.sample_queries` and each topic's `sample_queries`; skills come from `model.skills` and each
    topic's `skills`, deduped by id with topic skills overriding model skills. Each entry's `prompt` is
    ready to submit verbatim to `POST /api/v1/ai/jobs`.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiAgentActionsResponse | Any]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> AiAgentActionsResponse | Any | None:
    """Get model AI agent actions

     Returns the AI agent actions configured for this model — a unified list of sample queries and skills
    suitable for surfacing as suggested prompts above an AI prompt input. Sample queries come from both
    `model.sample_queries` and each topic's `sample_queries`; skills come from `model.skills` and each
    topic's `skills`, deduped by id with topic skills overriding model skills. Each entry's `prompt` is
    ready to submit verbatim to `POST /api/v1/ai/jobs`.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiAgentActionsResponse | Any
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiAgentActionsResponse | Any]:
    """Get model AI agent actions

     Returns the AI agent actions configured for this model — a unified list of sample queries and skills
    suitable for surfacing as suggested prompts above an AI prompt input. Sample queries come from both
    `model.sample_queries` and each topic's `sample_queries`; skills come from `model.skills` and each
    topic's `skills`, deduped by id with topic skills overriding model skills. Each entry's `prompt` is
    ready to submit verbatim to `POST /api/v1/ai/jobs`.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiAgentActionsResponse | Any]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> AiAgentActionsResponse | Any | None:
    """Get model AI agent actions

     Returns the AI agent actions configured for this model — a unified list of sample queries and skills
    suitable for surfacing as suggested prompts above an AI prompt input. Sample queries come from both
    `model.sample_queries` and each topic's `sample_queries`; skills come from `model.skills` and each
    topic's `skills`, deduped by id with topic skills overriding model skills. Each entry's `prompt` is
    ready to submit verbatim to `POST /api/v1/ai/jobs`.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiAgentActionsResponse | Any
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
        )
    ).parsed
