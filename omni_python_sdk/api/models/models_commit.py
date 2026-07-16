from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_commit_body import ModelsCommitBody
from ...models.models_commit_response import ModelsCommitResponse
from ...types import Response


def _get_kwargs(
    model_id: UUID,
    *,
    body: ModelsCommitBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/git/commit".format(
            model_id=quote(str(model_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsCommitResponse | None:
    if response.status_code == 200:
        response_200 = ModelsCommitResponse.from_dict(response.json())

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
) -> Response[Any | ModelsCommitResponse]:
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
    body: ModelsCommitBody,
) -> Response[Any | ModelsCommitResponse]:
    """Commit branch to git

     Push the branch contents to git and create or update a pull request. The backend automatically
    detects whether the git branch already exists: if not, it creates a new git branch and opens a PR;
    if it does, it commits the latest model contents to the existing branch (updating the open PR).

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsCommitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCommitResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCommitBody,
) -> Any | ModelsCommitResponse | None:
    """Commit branch to git

     Push the branch contents to git and create or update a pull request. The backend automatically
    detects whether the git branch already exists: if not, it creates a new git branch and opens a PR;
    if it does, it commits the latest model contents to the existing branch (updating the open PR).

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsCommitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCommitResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCommitBody,
) -> Response[Any | ModelsCommitResponse]:
    """Commit branch to git

     Push the branch contents to git and create or update a pull request. The backend automatically
    detects whether the git branch already exists: if not, it creates a new git branch and opens a PR;
    if it does, it commits the latest model contents to the existing branch (updating the open PR).

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsCommitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCommitResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCommitBody,
) -> Any | ModelsCommitResponse | None:
    """Commit branch to git

     Push the branch contents to git and create or update a pull request. The backend automatically
    detects whether the git branch already exists: if not, it creates a new git branch and opens a PR;
    if it does, it commits the latest model contents to the existing branch (updating the open PR).

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsCommitBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCommitResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            body=body,
        )
    ).parsed
