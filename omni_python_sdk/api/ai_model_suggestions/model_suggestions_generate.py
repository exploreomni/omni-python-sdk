from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.generate_suggestions_response import GenerateSuggestionsResponse
from ...models.suggestions_cooldown_response import SuggestionsCooldownResponse
from ...types import Response


def _get_kwargs(
    model_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/suggestions/generate".format(
            model_id=quote(str(model_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse | None:
    if response.status_code == 202:
        response_202 = GenerateSuggestionsResponse.from_dict(response.json())

        return response_202

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

    if response.status_code == 405:
        response_405 = cast(Any, None)
        return response_405

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 429:
        response_429 = SuggestionsCooldownResponse.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = cast(Any, None)
        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse]:
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
) -> Response[Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse]:
    """Generate model suggestions

     Triggers an AI suggestion generation run for the shared model and enqueues the async job. Poll `GET
    /suggestions/runs/{runId}` for status. Requires organization admin permissions. At most one active
    run per model; a cooldown applies after a completed run.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse]
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
) -> Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse | None:
    """Generate model suggestions

     Triggers an AI suggestion generation run for the shared model and enqueues the async job. Poll `GET
    /suggestions/runs/{runId}` for status. Requires organization admin permissions. At most one active
    run per model; a cooldown applies after a completed run.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse]:
    """Generate model suggestions

     Triggers an AI suggestion generation run for the shared model and enqueues the async job. Poll `GET
    /suggestions/runs/{runId}` for status. Requires organization admin permissions. At most one active
    run per model; a cooldown applies after a completed run.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse]
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
) -> Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse | None:
    """Generate model suggestions

     Triggers an AI suggestion generation run for the shared model and enqueues the async job. Poll `GET
    /suggestions/runs/{runId}` for status. Requires organization admin permissions. At most one active
    run per model; a cooldown applies after a completed run.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | GenerateSuggestionsResponse | SuggestionsCooldownResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
        )
    ).parsed
