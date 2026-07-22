from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.eval_api_error_400 import EvalApiError400
from ...models.eval_api_error_401 import EvalApiError401
from ...models.eval_api_error_403 import EvalApiError403
from ...models.eval_api_error_422 import EvalApiError422
from ...models.eval_prompt_sets_create_body import EvalPromptSetsCreateBody
from ...models.eval_prompt_sets_create_response import EvalPromptSetsCreateResponse
from ...types import Response


def _get_kwargs(
    *,
    body: EvalPromptSetsCreateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/eval/prompt-sets",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse | None:
    if response.status_code == 201:
        response_201 = EvalPromptSetsCreateResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = EvalApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = EvalApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = EvalApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 422:
        response_422 = EvalApiError422.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsCreateBody,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse]:
    """Create an eval prompt set

     Create a new eval prompt set bound to a shared model. Initial prompts can be supplied; additional
    prompts can be added later via PATCH.

    Args:
        body (EvalPromptSetsCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsCreateBody,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse | None:
    """Create an eval prompt set

     Create a new eval prompt set bound to a shared model. Initial prompts can be supplied; additional
    prompts can be added later via PATCH.

    Args:
        body (EvalPromptSetsCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsCreateBody,
) -> Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse]:
    """Create an eval prompt set

     Create a new eval prompt set bound to a shared model. Initial prompts can be supplied; additional
    prompts can be added later via PATCH.

    Args:
        body (EvalPromptSetsCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: EvalPromptSetsCreateBody,
) -> EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse | None:
    """Create an eval prompt set

     Create a new eval prompt set bound to a shared model. Initial prompts can be supplied; additional
    prompts can be added later via PATCH.

    Args:
        body (EvalPromptSetsCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EvalApiError400 | EvalApiError401 | EvalApiError403 | EvalApiError422 | EvalPromptSetsCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
