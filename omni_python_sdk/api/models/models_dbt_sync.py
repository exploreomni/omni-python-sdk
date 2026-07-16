from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.job_created_response import JobCreatedResponse
from ...types import UNSET, Response


def _get_kwargs(
    model_id: UUID,
    *,
    branch_id: UUID,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/dbt-sync".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | JobCreatedResponse | None:
    if response.status_code == 200:
        response_200 = JobCreatedResponse.from_dict(response.json())

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

    if response.status_code == 405:
        response_405 = cast(Any, None)
        return response_405

    if response.status_code == 422:
        response_422 = cast(Any, None)
        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | JobCreatedResponse]:
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
    branch_id: UUID,
) -> Response[Any | JobCreatedResponse]:
    r"""Trigger a dbt metadata sync for a branch

     Trigger a dbt metadata sync (\"dbt quick sync\") for a branch. Recompiles the branch's dbt manifest
    and merges the regenerated dbt extension model, without a full database schema scan. The branch (via
    branch_id) supplies the dbt environment and dbt git branch. Runs as a background job.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID): ID of the branch to sync dbt metadata for. The branch supplies the dbt
            environment and dbt git branch to compile against (set via POST
            /api/v1/models/{modelId}/branch/{branchName}/dbt). Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | JobCreatedResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID,
) -> Any | JobCreatedResponse | None:
    r"""Trigger a dbt metadata sync for a branch

     Trigger a dbt metadata sync (\"dbt quick sync\") for a branch. Recompiles the branch's dbt manifest
    and merges the regenerated dbt extension model, without a full database schema scan. The branch (via
    branch_id) supplies the dbt environment and dbt git branch. Runs as a background job.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID): ID of the branch to sync dbt metadata for. The branch supplies the dbt
            environment and dbt git branch to compile against (set via POST
            /api/v1/models/{modelId}/branch/{branchName}/dbt). Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | JobCreatedResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        branch_id=branch_id,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID,
) -> Response[Any | JobCreatedResponse]:
    r"""Trigger a dbt metadata sync for a branch

     Trigger a dbt metadata sync (\"dbt quick sync\") for a branch. Recompiles the branch's dbt manifest
    and merges the regenerated dbt extension model, without a full database schema scan. The branch (via
    branch_id) supplies the dbt environment and dbt git branch. Runs as a background job.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID): ID of the branch to sync dbt metadata for. The branch supplies the dbt
            environment and dbt git branch to compile against (set via POST
            /api/v1/models/{modelId}/branch/{branchName}/dbt). Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | JobCreatedResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID,
) -> Any | JobCreatedResponse | None:
    r"""Trigger a dbt metadata sync for a branch

     Trigger a dbt metadata sync (\"dbt quick sync\") for a branch. Recompiles the branch's dbt manifest
    and merges the regenerated dbt extension model, without a full database schema scan. The branch (via
    branch_id) supplies the dbt environment and dbt git branch. Runs as a background job.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID): ID of the branch to sync dbt metadata for. The branch supplies the dbt
            environment and dbt git branch to compile against (set via POST
            /api/v1/models/{modelId}/branch/{branchName}/dbt). Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | JobCreatedResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            branch_id=branch_id,
        )
    ).parsed
