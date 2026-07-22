from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.suggestion_run import SuggestionRun
from ...types import Response


def _get_kwargs(
    model_id: UUID,
    run_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/suggestions/runs/{run_id}".format(
            model_id=quote(str(model_id), safe=""),
            run_id=quote(str(run_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | SuggestionRun | None:
    if response.status_code == 200:
        response_200 = SuggestionRun.from_dict(response.json())

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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | SuggestionRun]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | SuggestionRun]:
    """Get a generation run

     Returns the status of a specific generation run. Poll until `status` is terminal (`complete` or
    `failed`), then re-fetch the suggestions list. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the run belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        run_id (UUID): UUID of the generation run Example: b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuggestionRun]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        run_id=run_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | SuggestionRun | None:
    """Get a generation run

     Returns the status of a specific generation run. Poll until `status` is terminal (`complete` or
    `failed`), then re-fetch the suggestions list. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the run belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        run_id (UUID): UUID of the generation run Example: b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuggestionRun
    """

    return sync_detailed(
        model_id=model_id,
        run_id=run_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | SuggestionRun]:
    """Get a generation run

     Returns the status of a specific generation run. Poll until `status` is terminal (`complete` or
    `failed`), then re-fetch the suggestions list. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the run belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        run_id (UUID): UUID of the generation run Example: b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuggestionRun]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        run_id=run_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | SuggestionRun | None:
    """Get a generation run

     Returns the status of a specific generation run. Poll until `status` is terminal (`complete` or
    `failed`), then re-fetch the suggestions list. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the run belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        run_id (UUID): UUID of the generation run Example: b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuggestionRun
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            run_id=run_id,
            client=client,
        )
    ).parsed
