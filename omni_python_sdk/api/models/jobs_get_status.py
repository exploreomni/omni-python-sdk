from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.jobs_get_status_response import JobsGetStatusResponse
from ...types import Response


def _get_kwargs(
    job_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/jobs/{job_id}/status".format(
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | JobsGetStatusResponse | None:
    if response.status_code == 200:
        response_200 = JobsGetStatusResponse.from_dict(response.json())

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


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | JobsGetStatusResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    job_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | JobsGetStatusResponse]:
    """Get schema refresh or dbt sync job status

     Check status of a schema refresh job (POST /api/v1/models/{modelId}/refresh) or a dbt sync job (POST
    /api/v1/models/{modelId}/dbt-sync). Returns IN_PROGRESS, COMPLETED, or FAILED.

    Args:
        job_id (str): The job ID returned from a job creation endpoint (e.g., POST
            /api/v1/models/{modelId}/refresh) Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | JobsGetStatusResponse]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | JobsGetStatusResponse | None:
    """Get schema refresh or dbt sync job status

     Check status of a schema refresh job (POST /api/v1/models/{modelId}/refresh) or a dbt sync job (POST
    /api/v1/models/{modelId}/dbt-sync). Returns IN_PROGRESS, COMPLETED, or FAILED.

    Args:
        job_id (str): The job ID returned from a job creation endpoint (e.g., POST
            /api/v1/models/{modelId}/refresh) Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | JobsGetStatusResponse
    """

    return sync_detailed(
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    job_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | JobsGetStatusResponse]:
    """Get schema refresh or dbt sync job status

     Check status of a schema refresh job (POST /api/v1/models/{modelId}/refresh) or a dbt sync job (POST
    /api/v1/models/{modelId}/dbt-sync). Returns IN_PROGRESS, COMPLETED, or FAILED.

    Args:
        job_id (str): The job ID returned from a job creation endpoint (e.g., POST
            /api/v1/models/{modelId}/refresh) Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | JobsGetStatusResponse]
    """

    kwargs = _get_kwargs(
        job_id=job_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | JobsGetStatusResponse | None:
    """Get schema refresh or dbt sync job status

     Check status of a schema refresh job (POST /api/v1/models/{modelId}/refresh) or a dbt sync job (POST
    /api/v1/models/{modelId}/dbt-sync). Returns IN_PROGRESS, COMPLETED, or FAILED.

    Args:
        job_id (str): The job ID returned from a job creation endpoint (e.g., POST
            /api/v1/models/{modelId}/refresh) Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | JobsGetStatusResponse
    """

    return (
        await asyncio_detailed(
            job_id=job_id,
            client=client,
        )
    ).parsed
