from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.query_stream_footer_line import QueryStreamFooterLine
from ...models.query_stream_job_line import QueryStreamJobLine
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
) -> Any | QueryStreamFooterLine | QueryStreamJobLine | None:
    if response.status_code == 200:

        def _parse_response_200(data: object) -> QueryStreamFooterLine | QueryStreamJobLine:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_wait_stream_line_type_0 = QueryStreamJobLine.from_dict(data)

                return componentsschemas_query_wait_stream_line_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_wait_stream_line_type_1 = QueryStreamFooterLine.from_dict(data)

            return componentsschemas_query_wait_stream_line_type_1

        response_200 = _parse_response_200(response.text)

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
) -> Response[Any | QueryStreamFooterLine | QueryStreamJobLine]:
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
) -> Response[Any | QueryStreamFooterLine | QueryStreamJobLine]:
    """Wait for query jobs to complete

     Waits for previously submitted query jobs and streams results as they complete. The response is a
    stream of newline-delimited JSON (`Content-Type: text/ndjson`): one line per job (same shape as the
    job lines from query/run, including the base64-encoded Arrow IPC `result`), then a footer. Unlike
    query/run, there is no `jobs_submitted` header line. If the footer's `remaining_job_ids` is non-
    empty, call this endpoint again with those IDs until it is empty.

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryStreamFooterLine | QueryStreamJobLine]
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
) -> Any | QueryStreamFooterLine | QueryStreamJobLine | None:
    """Wait for query jobs to complete

     Waits for previously submitted query jobs and streams results as they complete. The response is a
    stream of newline-delimited JSON (`Content-Type: text/ndjson`): one line per job (same shape as the
    job lines from query/run, including the base64-encoded Arrow IPC `result`), then a footer. Unlike
    query/run, there is no `jobs_submitted` header line. If the footer's `remaining_job_ids` is non-
    empty, call this endpoint again with those IDs until it is empty.

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryStreamFooterLine | QueryStreamJobLine
    """

    return sync_detailed(
        client=client,
        job_ids=job_ids,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    job_ids: str,
) -> Response[Any | QueryStreamFooterLine | QueryStreamJobLine]:
    """Wait for query jobs to complete

     Waits for previously submitted query jobs and streams results as they complete. The response is a
    stream of newline-delimited JSON (`Content-Type: text/ndjson`): one line per job (same shape as the
    job lines from query/run, including the base64-encoded Arrow IPC `result`), then a footer. Unlike
    query/run, there is no `jobs_submitted` header line. If the footer's `remaining_job_ids` is non-
    empty, call this endpoint again with those IDs until it is empty.

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryStreamFooterLine | QueryStreamJobLine]
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
) -> Any | QueryStreamFooterLine | QueryStreamJobLine | None:
    """Wait for query jobs to complete

     Waits for previously submitted query jobs and streams results as they complete. The response is a
    stream of newline-delimited JSON (`Content-Type: text/ndjson`): one line per job (same shape as the
    job lines from query/run, including the base64-encoded Arrow IPC `result`), then a footer. Unlike
    query/run, there is no `jobs_submitted` header line. If the footer's `remaining_job_ids` is non-
    empty, call this endpoint again with those IDs until it is empty.

    Args:
        job_ids (str): Comma-separated list of job IDs to wait for. Obtained from the query/run
            response. Example: job_abc123,job_def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryStreamFooterLine | QueryStreamJobLine
    """

    return (
        await asyncio_detailed(
            client=client,
            job_ids=job_ids,
        )
    ).parsed
