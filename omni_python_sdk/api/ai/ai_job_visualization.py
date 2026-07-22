from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.api_error_422 import ApiError422
from ...types import Response


def _get_kwargs(
    job_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/jobs/{job_id}/vis".format(
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422 | None:
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

    if response.status_code == 422:
        response_422 = ApiError422.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422]:
    r"""Render AI job visualization

     Render the visualization from a completed AI job as a PNG image. The endpoint extracts the
    visualization configuration from the job result, loads Arrow IPC data, and renders it server-side
    using Vega. For style-only follow-ups (e.g., \"make it a bar chart\"), the endpoint walks back
    through previous jobs in the conversation to find the original query data.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422 | None:
    r"""Render AI job visualization

     Render the visualization from a completed AI job as a PNG image. The endpoint extracts the
    visualization configuration from the job result, loads Arrow IPC data, and renders it server-side
    using Vega. For style-only follow-ups (e.g., \"make it a bar chart\"), the endpoint walks back
    through previous jobs in the conversation to find the original query data.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422
    """

    return sync_detailed(
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422]:
    r"""Render AI job visualization

     Render the visualization from a completed AI job as a PNG image. The endpoint extracts the
    visualization configuration from the job result, loads Arrow IPC data, and renders it server-side
    using Vega. For style-only follow-ups (e.g., \"make it a bar chart\"), the endpoint walks back
    through previous jobs in the conversation to find the original query data.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422 | None:
    r"""Render AI job visualization

     Render the visualization from a completed AI job as a PNG image. The endpoint extracts the
    visualization configuration from the job result, loads Arrow IPC data, and renders it server-side
    using Vega. For style-only follow-ups (e.g., \"make it a bar chart\"), the endpoint walks back
    through previous jobs in the conversation to find the original query data.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError422
    """

    return (
        await asyncio_detailed(
            job_id=job_id,
            client=client,
        )
    ).parsed
