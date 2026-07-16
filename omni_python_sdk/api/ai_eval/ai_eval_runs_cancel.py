from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.eval_api_error_401 import EvalApiError401
from ...models.eval_api_error_403 import EvalApiError403
from ...models.eval_api_error_404 import EvalApiError404
from ...models.eval_api_error_500 import EvalApiError500
from ...models.eval_runs_cancel_response import EvalRunsCancelResponse
from ...types import Response


def _get_kwargs(
    run_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/eval/runs/{run_id}/cancel".format(
            run_id=quote(str(run_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse | None:
    if response.status_code == 200:
        response_200 = EvalRunsCancelResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = EvalApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = EvalApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = EvalApiError404.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = EvalApiError500.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse]:
    """Cancel an eval run

     Cancel an in-flight eval run. Any non-terminal per-prompt jobs are cancelled and the run is archived
    — the response returns the updated run inline (`status: CANCELLED`, `is_archived: true`); use
    `/unarchive` to surface it in the default `archived=false` list again.

    Args:
        run_id (UUID): The unique identifier of the eval run. Example:
            660e8400-e29b-41d4-a716-446655440001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse]
    """

    kwargs = _get_kwargs(
        run_id=run_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse | None:
    """Cancel an eval run

     Cancel an in-flight eval run. Any non-terminal per-prompt jobs are cancelled and the run is archived
    — the response returns the updated run inline (`status: CANCELLED`, `is_archived: true`); use
    `/unarchive` to surface it in the default `archived=false` list again.

    Args:
        run_id (UUID): The unique identifier of the eval run. Example:
            660e8400-e29b-41d4-a716-446655440001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse
    """

    return sync_detailed(
        run_id=run_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse]:
    """Cancel an eval run

     Cancel an in-flight eval run. Any non-terminal per-prompt jobs are cancelled and the run is archived
    — the response returns the updated run inline (`status: CANCELLED`, `is_archived: true`); use
    `/unarchive` to surface it in the default `archived=false` list again.

    Args:
        run_id (UUID): The unique identifier of the eval run. Example:
            660e8400-e29b-41d4-a716-446655440001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse]
    """

    kwargs = _get_kwargs(
        run_id=run_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    run_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse | None:
    """Cancel an eval run

     Cancel an in-flight eval run. Any non-terminal per-prompt jobs are cancelled and the run is archived
    — the response returns the updated run inline (`status: CANCELLED`, `is_archived: true`); use
    `/unarchive` to surface it in the default `archived=false` list again.

    Args:
        run_id (UUID): The unique identifier of the eval run. Example:
            660e8400-e29b-41d4-a716-446655440001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError500 | EvalRunsCancelResponse
    """

    return (
        await asyncio_detailed(
            run_id=run_id,
            client=client,
        )
    ).parsed
