from http import HTTPStatus
from typing import Any
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_eval_prompt_sets_list_archived import (
    AiEvalPromptSetsListArchived,
)
from ...models.eval_api_error_400 import EvalApiError400
from ...models.eval_api_error_401 import EvalApiError401
from ...models.eval_api_error_403 import EvalApiError403
from ...models.eval_api_error_404 import EvalApiError404
from ...models.eval_prompt_sets_list_response import EvalPromptSetsListResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    archived: AiEvalPromptSetsListArchived | Unset = UNSET,
    model_ids: list[UUID] | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_archived: str | Unset = UNSET
    if not isinstance(archived, Unset):
        json_archived = archived

    params["archived"] = json_archived

    json_model_ids: list[str] | Unset = UNSET
    if not isinstance(model_ids, Unset):
        json_model_ids = []
        for model_ids_item_data in model_ids:
            model_ids_item = str(model_ids_item_data)
            json_model_ids.append(model_ids_item)

    params["model_ids"] = json_model_ids

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/eval/prompt-sets",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse | None:
    if response.status_code == 200:
        response_200 = EvalPromptSetsListResponse.from_dict(response.json())

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
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    archived: AiEvalPromptSetsListArchived | Unset = UNSET,
    model_ids: list[UUID] | Unset = UNSET,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse]:
    """List eval prompt sets

     List eval prompt sets, sorted alphabetically by name. When `model_ids` is omitted, returns prompt
    sets for every shared model the caller can access. Requires at least the Querier role on each
    requested model.

    Args:
        archived (AiEvalPromptSetsListArchived | Unset): When `true`, returns archived prompt sets
            instead of active ones. Defaults to `false`. Example: false.
        model_ids (list[UUID] | Unset): Optional list of model IDs to filter prompt sets by. When
            omitted, returns prompt sets for every model the caller can access. Supply multiple times
            to filter by more than one model (e.g., `?model_ids=A&model_ids=B`).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse]
    """

    kwargs = _get_kwargs(
        archived=archived,
        model_ids=model_ids,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    archived: AiEvalPromptSetsListArchived | Unset = UNSET,
    model_ids: list[UUID] | Unset = UNSET,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse | None:
    """List eval prompt sets

     List eval prompt sets, sorted alphabetically by name. When `model_ids` is omitted, returns prompt
    sets for every shared model the caller can access. Requires at least the Querier role on each
    requested model.

    Args:
        archived (AiEvalPromptSetsListArchived | Unset): When `true`, returns archived prompt sets
            instead of active ones. Defaults to `false`. Example: false.
        model_ids (list[UUID] | Unset): Optional list of model IDs to filter prompt sets by. When
            omitted, returns prompt sets for every model the caller can access. Supply multiple times
            to filter by more than one model (e.g., `?model_ids=A&model_ids=B`).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse
    """

    return sync_detailed(
        client=client,
        archived=archived,
        model_ids=model_ids,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    archived: AiEvalPromptSetsListArchived | Unset = UNSET,
    model_ids: list[UUID] | Unset = UNSET,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse]:
    """List eval prompt sets

     List eval prompt sets, sorted alphabetically by name. When `model_ids` is omitted, returns prompt
    sets for every shared model the caller can access. Requires at least the Querier role on each
    requested model.

    Args:
        archived (AiEvalPromptSetsListArchived | Unset): When `true`, returns archived prompt sets
            instead of active ones. Defaults to `false`. Example: false.
        model_ids (list[UUID] | Unset): Optional list of model IDs to filter prompt sets by. When
            omitted, returns prompt sets for every model the caller can access. Supply multiple times
            to filter by more than one model (e.g., `?model_ids=A&model_ids=B`).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse]
    """

    kwargs = _get_kwargs(
        archived=archived,
        model_ids=model_ids,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    archived: AiEvalPromptSetsListArchived | Unset = UNSET,
    model_ids: list[UUID] | Unset = UNSET,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse | None:
    """List eval prompt sets

     List eval prompt sets, sorted alphabetically by name. When `model_ids` is omitted, returns prompt
    sets for every shared model the caller can access. Requires at least the Querier role on each
    requested model.

    Args:
        archived (AiEvalPromptSetsListArchived | Unset): When `true`, returns archived prompt sets
            instead of active ones. Defaults to `false`. Example: false.
        model_ids (list[UUID] | Unset): Optional list of model IDs to filter prompt sets by. When
            omitted, returns prompt sets for every model the caller can access. Supply multiple times
            to filter by more than one model (e.g., `?model_ids=A&model_ids=B`).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError404 | EvalPromptSetsListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            archived=archived,
            model_ids=model_ids,
        )
    ).parsed
