from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.eval_api_error_400 import EvalApiError400
from ...models.eval_api_error_401 import EvalApiError401
from ...models.eval_api_error_403 import EvalApiError403
from ...models.eval_api_error_404 import EvalApiError404
from ...models.eval_prompt_sets_unarchive_response import EvalPromptSetsUnarchiveResponse
from ...types import Response


def _get_kwargs(
    prompt_set_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/eval/prompt-sets/{prompt_set_id}/unarchive".format(
            prompt_set_id=quote(str(prompt_set_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse | None:
    if response.status_code == 200:
        response_200 = EvalPromptSetsUnarchiveResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = EvalApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = EvalApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = EvalApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = EvalApiError404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse]:
    """Restore an archived eval prompt set

     Restore an archived prompt set.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse]
    """

    kwargs = _get_kwargs(
        prompt_set_id=prompt_set_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse | None:
    """Restore an archived eval prompt set

     Restore an archived prompt set.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse
    """

    return sync_detailed(
        prompt_set_id=prompt_set_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse]:
    """Restore an archived eval prompt set

     Restore an archived prompt set.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse]
    """

    kwargs = _get_kwargs(
        prompt_set_id=prompt_set_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse | None:
    """Restore an archived eval prompt set

     Restore an archived prompt set.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsUnarchiveResponse
    """

    return (
        await asyncio_detailed(
            prompt_set_id=prompt_set_id,
            client=client,
        )
    ).parsed
