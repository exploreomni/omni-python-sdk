from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_pick_topic_body import AiPickTopicBody
from ...models.ai_pick_topic_response import AiPickTopicResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...types import Response


def _get_kwargs(
    *,
    body: AiPickTopicBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/pick-topic",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    if response.status_code == 200:
        response_200 = AiPickTopicResponse.from_dict(response.json())

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

    if response.status_code == 500:
        response_500 = cast(Any, None)
        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiPickTopicBody,
) -> Response[AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Pick the best topic for a prompt

     Analyze a natural language prompt and determine which topic in the model is the best fit for
    answering the question. Useful as a preprocessing step before calling generate-query or submitting
    an AI job, especially when the user's question could relate to multiple topics.

    Args:
        body (AiPickTopicBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: AiPickTopicBody,
) -> AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Pick the best topic for a prompt

     Analyze a natural language prompt and determine which topic in the model is the best fit for
    answering the question. Useful as a preprocessing step before calling generate-query or submitting
    an AI job, especially when the user's question could relate to multiple topics.

    Args:
        body (AiPickTopicBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiPickTopicBody,
) -> Response[AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Pick the best topic for a prompt

     Analyze a natural language prompt and determine which topic in the model is the best fit for
    answering the question. Useful as a preprocessing step before calling generate-query or submitting
    an AI job, especially when the user's question could relate to multiple topics.

    Args:
        body (AiPickTopicBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiPickTopicBody,
) -> AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Pick the best topic for a prompt

     Analyze a natural language prompt and determine which topic in the model is the best fit for
    answering the question. Useful as a preprocessing step before calling generate-query or submitting
    an AI job, especially when the user's question could relate to multiple topics.

    Args:
        body (AiPickTopicBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiPickTopicResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
