from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_job_cancel_response import AiJobCancelResponse
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.api_error_409 import ApiError409
from ...types import Response


def _get_kwargs(
    job_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/jobs/{job_id}/cancel".format(
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    if response.status_code == 200:
        response_200 = AiJobCancelResponse.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = ApiError409.from_dict(response.json())

        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
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
) -> Response[AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
    """Cancel an AI job

     Request cancellation of an AI job. This endpoint is idempotent — calling it on an already-cancelled
    or completed job returns success with the current state. For QUEUED jobs, cancellation is immediate.
    For EXECUTING jobs, the worker will stop after completing its current iteration. Jobs in DELIVERING
    state cannot be cancelled as they are already finalizing results. Only the job owner or organization
    admins can cancel jobs.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]
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
) -> AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    """Cancel an AI job

     Request cancellation of an AI job. This endpoint is idempotent — calling it on an already-cancelled
    or completed job returns success with the current state. For QUEUED jobs, cancellation is immediate.
    For EXECUTING jobs, the worker will stop after completing its current iteration. Jobs in DELIVERING
    state cannot be cancelled as they are already finalizing results. Only the job owner or organization
    admins can cancel jobs.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409
    """

    return sync_detailed(
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    job_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]:
    """Cancel an AI job

     Request cancellation of an AI job. This endpoint is idempotent — calling it on an already-cancelled
    or completed job returns success with the current state. For QUEUED jobs, cancellation is immediate.
    For EXECUTING jobs, the worker will stop after completing its current iteration. Jobs in DELIVERING
    state cannot be cancelled as they are already finalizing results. Only the job owner or organization
    admins can cancel jobs.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409]
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
) -> AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409 | None:
    """Cancel an AI job

     Request cancellation of an AI job. This endpoint is idempotent — calling it on an already-cancelled
    or completed job returns success with the current state. For QUEUED jobs, cancellation is immediate.
    For EXECUTING jobs, the worker will stop after completing its current iteration. Jobs in DELIVERING
    state cannot be cancelled as they are already finalizing results. Only the job owner or organization
    admins can cancel jobs.

    Args:
        job_id (UUID): The unique identifier of the AI job Example:
            123e4567-e89b-12d3-a456-426614174000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiJobCancelResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError409
    """

    return (
        await asyncio_detailed(
            job_id=job_id,
            client=client,
        )
    ).parsed
