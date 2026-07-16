from http import HTTPStatus
from typing import Any
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.api_error_429 import ApiError429
from ...models.routine_create_body import RoutineCreateBody
from ...models.routine_create_response import RoutineCreateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: RoutineCreateBody,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/ai/routines",
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse | None:
    if response.status_code == 201:
        response_201 = RoutineCreateResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = ApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ApiError404.from_dict(response.json())

        return response_404

    if response.status_code == 429:
        response_429 = ApiError429.from_dict(response.json())

        return response_429

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: RoutineCreateBody,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse]:
    """Create a routine

     Create a routine that runs a saved prompt on a schedule and delivers the AI response through a
    single destination — email (one or more recipients / user groups) or Slack (a single channel or
    direct message). Each scheduled run executes once using the routine owner's permissions, and every
    recipient receives the same result. Organization API keys can pass `?userId=<membershipId>` to
    create the routine for a specific organization member.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (RoutineCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: RoutineCreateBody,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse | None:
    """Create a routine

     Create a routine that runs a saved prompt on a schedule and delivers the AI response through a
    single destination — email (one or more recipients / user groups) or Slack (a single channel or
    direct message). Each scheduled run executes once using the routine owner's permissions, and every
    recipient receives the same result. Organization API keys can pass `?userId=<membershipId>` to
    create the routine for a specific organization member.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (RoutineCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: RoutineCreateBody,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse]:
    """Create a routine

     Create a routine that runs a saved prompt on a schedule and delivers the AI response through a
    single destination — email (one or more recipients / user groups) or Slack (a single channel or
    direct message). Each scheduled run executes once using the routine owner's permissions, and every
    recipient receives the same result. Organization API keys can pass `?userId=<membershipId>` to
    create the routine for a specific organization member.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (RoutineCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: RoutineCreateBody,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse | None:
    """Create a routine

     Create a routine that runs a saved prompt on a schedule and delivers the AI response through a
    single destination — email (one or more recipients / user groups) or Slack (a single channel or
    direct message). Each scheduled run executes once using the routine owner's permissions, and every
    recipient receives the same result. Organization API keys can pass `?userId=<membershipId>` to
    create the routine for a specific organization member.

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (RoutineCreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | ApiError429 | RoutineCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed
