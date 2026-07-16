from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_job_status_response import AiJobStatusResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...types import Response


def _get_kwargs(
    job_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/jobs/{job_id}".format(
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    if response.status_code == 200:
        response_200 = AiJobStatusResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
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
) -> Response[AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Get AI job status

     Get the current status of an AI job, including its state, progress information, and result summary.
    The response fields vary by state — for example, progress is only present during EXECUTING, and
    resultSummary is only present when COMPLETE. Poll this endpoint every 2–5 seconds until the job
    reaches a terminal state (COMPLETE, FAILED, or CANCELLED).

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]
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
) -> AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Get AI job status

     Get the current status of an AI job, including its state, progress information, and result summary.
    The response fields vary by state — for example, progress is only present during EXECUTING, and
    resultSummary is only present when COMPLETE. Poll this endpoint every 2–5 seconds until the job
    reaches a terminal state (COMPLETE, FAILED, or CANCELLED).

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return sync_detailed(
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Get AI job status

     Get the current status of an AI job, including its state, progress information, and result summary.
    The response fields vary by state — for example, progress is only present during EXECUTING, and
    resultSummary is only present when COMPLETE. Poll this endpoint every 2–5 seconds until the job
    reaches a terminal state (COMPLETE, FAILED, or CANCELLED).

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]
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
) -> AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Get AI job status

     Get the current status of an AI job, including its state, progress information, and result summary.
    The response fields vary by state — for example, progress is only present during EXECUTING, and
    resultSummary is only present when COMPLETE. Poll this endpoint every 2–5 seconds until the job
    reaches a terminal state (COMPLETE, FAILED, or CANCELLED).

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobStatusResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return (
        await asyncio_detailed(
            job_id=job_id,
            client=client,
        )
    ).parsed
