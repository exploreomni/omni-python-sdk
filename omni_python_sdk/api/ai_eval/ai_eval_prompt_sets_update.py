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
from ...models.eval_api_error_422 import EvalApiError422
from ...models.eval_prompt_sets_update_body import EvalPromptSetsUpdateBody
from ...models.eval_prompt_sets_update_response import EvalPromptSetsUpdateResponse
from ...types import Response


def _get_kwargs(
    prompt_set_id: UUID,
    *,
    body: EvalPromptSetsUpdateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/ai/eval/prompt-sets/{prompt_set_id}".format(
            prompt_set_id=quote(str(prompt_set_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
    | None
):
    if response.status_code == 200:
        response_200 = EvalPromptSetsUpdateResponse.from_dict(response.json())

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

    if response.status_code == 422:
        response_422 = EvalApiError422.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
]:
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
    body: EvalPromptSetsUpdateBody,
) -> Response[
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
]:
    """Update an eval prompt set

     Update a prompt set's name, description, and/or prompts. When `prompts` is supplied, it fully
    replaces the existing list — existing prompts omitted from the list are deleted, entries without an
    `id` are created, and entries with a matching `id` are updated in place.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (EvalPromptSetsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError422 | EvalPromptSetsUpdateResponse]
    """

    kwargs = _get_kwargs(
        prompt_set_id=prompt_set_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsUpdateBody,
) -> (
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
    | None
):
    """Update an eval prompt set

     Update a prompt set's name, description, and/or prompts. When `prompts` is supplied, it fully
    replaces the existing list — existing prompts omitted from the list are deleted, entries without an
    `id` are created, and entries with a matching `id` are updated in place.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (EvalPromptSetsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError422 | EvalPromptSetsUpdateResponse
    """

    return sync_detailed(
        prompt_set_id=prompt_set_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsUpdateBody,
) -> Response[
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
]:
    """Update an eval prompt set

     Update a prompt set's name, description, and/or prompts. When `prompts` is supplied, it fully
    replaces the existing list — existing prompts omitted from the list are deleted, entries without an
    `id` are created, and entries with a matching `id` are updated in place.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (EvalPromptSetsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError422 | EvalPromptSetsUpdateResponse]
    """

    kwargs = _get_kwargs(
        prompt_set_id=prompt_set_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    prompt_set_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsUpdateBody,
) -> (
    EvalApiError400
    | EvalApiError401
    | EvalApiError403
    | EvalApiError404
    | EvalApiError422
    | EvalPromptSetsUpdateResponse
    | None
):
    """Update an eval prompt set

     Update a prompt set's name, description, and/or prompts. When `prompts` is supplied, it fully
    replaces the existing list — existing prompts omitted from the list are deleted, entries without an
    `id` are created, and entries with a matching `id` are updated in place.

    Args:
        prompt_set_id (UUID): The unique identifier of the eval prompt set. Example:
            550e8400-e29b-41d4-a716-446655440000.
        body (EvalPromptSetsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalApiError422 | EvalPromptSetsUpdateResponse
    """

    return (
        await asyncio_detailed(
            prompt_set_id=prompt_set_id,
            client=client,
            body=body,
        )
    ).parsed
