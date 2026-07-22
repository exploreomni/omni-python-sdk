from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_credit_shutoff_error import AiCreditShutoffError
from ...models.ai_generate_query_body import AiGenerateQueryBody
from ...models.ai_generate_query_response import AiGenerateQueryResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...types import Response


def _get_kwargs(
    *,
    body: AiGenerateQueryBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/generate-query",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None
):
    if response.status_code == 200:
        response_200 = AiGenerateQueryResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 402:
        response_402 = AiCreditShutoffError.from_dict(response.json())

        return response_402

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
) -> Response[
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiGenerateQueryBody,
) -> Response[
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
]:
    """Generate query from natural language

     Generate an Omni semantic query from a natural language prompt. Optionally executes the generated
    query and returns results. The AI analyzes the prompt, selects appropriate fields and filters from
    the model, and constructs a query. Requires the querier role on the target model. The effective
    user's per-connector AI toggles (set in the chat + menu) govern which integration tools the agent
    may use.

    Args:
        body (AiGenerateQueryBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]
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
    body: AiGenerateQueryBody,
) -> (
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None
):
    """Generate query from natural language

     Generate an Omni semantic query from a natural language prompt. Optionally executes the generated
    query and returns results. The AI analyzes the prompt, selects appropriate fields and filters from
    the model, and constructs a query. Requires the querier role on the target model. The effective
    user's per-connector AI toggles (set in the chat + menu) govern which integration tools the agent
    may use.

    Args:
        body (AiGenerateQueryBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiGenerateQueryBody,
) -> Response[
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
]:
    """Generate query from natural language

     Generate an Omni semantic query from a natural language prompt. Optionally executes the generated
    query and returns results. The AI analyzes the prompt, selects appropriate fields and filters from
    the model, and constructs a query. Requires the querier role on the target model. The effective
    user's per-connector AI toggles (set in the chat + menu) govern which integration tools the agent
    may use.

    Args:
        body (AiGenerateQueryBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiGenerateQueryBody,
) -> (
    AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None
):
    """Generate query from natural language

     Generate an Omni semantic query from a natural language prompt. Optionally executes the generated
    query and returns results. The AI analyzes the prompt, selects appropriate fields and filters from
    the model, and constructs a query. Requires the querier role on the target model. The effective
    user's per-connector AI toggles (set in the chat + menu) govern which integration tools the agent
    may use.

    Args:
        body (AiGenerateQueryBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditShutoffError | AiGenerateQueryResponse | Any | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
