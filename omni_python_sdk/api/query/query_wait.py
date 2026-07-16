from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.query_wait_response import QueryWaitResponse
from ...types import UNSET, Response


def _get_kwargs(
    *,
    job_ids: str,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["jobIds"] = job_ids

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/query/wait",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | QueryWaitResponse | None:
    if response.status_code == 200:
        response_200 = QueryWaitResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 404:
        response_404 = cast(Any, None)
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
) -> Response[Any | QueryWaitResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    job_ids: str,
) -> Response[Any | QueryWaitResponse]:
    """Wait for query jobs to complete

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryWaitResponse]
    """

    kwargs = _get_kwargs(
        job_ids=job_ids,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    job_ids: str,
) -> Any | QueryWaitResponse | None:
    """Wait for query jobs to complete

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryWaitResponse
    """

    return sync_detailed(
        client=client,
        job_ids=job_ids,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    job_ids: str,
) -> Response[Any | QueryWaitResponse]:
    """Wait for query jobs to complete

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryWaitResponse]
    """

    kwargs = _get_kwargs(
        job_ids=job_ids,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    job_ids: str,
) -> Any | QueryWaitResponse | None:
    """Wait for query jobs to complete

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryWaitResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            job_ids=job_ids,
        )
    ).parsed
