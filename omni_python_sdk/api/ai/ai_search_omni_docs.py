from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_search_omni_docs_body import AiSearchOmniDocsBody
from ...models.ai_search_omni_docs_response import AiSearchOmniDocsResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...types import Response


def _get_kwargs(
    *,
    body: AiSearchOmniDocsBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/search-omni-docs",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403 | None:
    if response.status_code == 200:
        response_200 = AiSearchOmniDocsResponse.from_dict(response.json())

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

    if response.status_code == 500:
        response_500 = cast(Any, None)
        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiSearchOmniDocsBody,
) -> Response[AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403]:
    """Search Omni documentation

     Search the Omni documentation using AI to answer questions about Omni features, configuration,
    modeling, dashboards, and more. Sends a natural language question and returns a synthesized answer
    with source links to the relevant documentation pages.

    Args:
        body (AiSearchOmniDocsBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403]
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
    body: AiSearchOmniDocsBody,
) -> AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403 | None:
    """Search Omni documentation

     Search the Omni documentation using AI to answer questions about Omni features, configuration,
    modeling, dashboards, and more. Sends a natural language question and returns a synthesized answer
    with source links to the relevant documentation pages.

    Args:
        body (AiSearchOmniDocsBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiSearchOmniDocsBody,
) -> Response[AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403]:
    """Search Omni documentation

     Search the Omni documentation using AI to answer questions about Omni features, configuration,
    modeling, dashboards, and more. Sends a natural language question and returns a synthesized answer
    with source links to the relevant documentation pages.

    Args:
        body (AiSearchOmniDocsBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiSearchOmniDocsBody,
) -> AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403 | None:
    """Search Omni documentation

     Search the Omni documentation using AI to answer questions about Omni features, configuration,
    modeling, dashboards, and more. Sends a natural language question and returns a synthesized answer
    with source links to the relevant documentation pages.

    Args:
        body (AiSearchOmniDocsBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiSearchOmniDocsResponse | Any | ApiError400 | ApiError401 | ApiError403
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
