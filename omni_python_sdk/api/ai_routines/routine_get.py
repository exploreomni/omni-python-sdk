from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...models.routine_response import RoutineResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: UUID,
    *,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/routines/{id}".format(
            id=quote(str(id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse | None:
    if response.status_code == 200:
        response_200 = RoutineResponse.from_dict(response.json())

        return response_200

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse]:
    """Get a routine

     Get a single routine, including the status of its most recent completed run.

    Args:
        id (UUID): The UUID of the routine.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse | None:
    """Get a routine

     Get a single routine, including the status of its most recent completed run.

    Args:
        id (UUID): The UUID of the routine.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse
    """

    return sync_detailed(
        id=id,
        client=client,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse]:
    """Get a routine

     Get a single routine, including the status of its most recent completed run.

    Args:
        id (UUID): The UUID of the routine.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse]
    """

    kwargs = _get_kwargs(
        id=id,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse | None:
    """Get a routine

     Get a single routine, including the status of its most recent completed run.

    Args:
        id (UUID): The UUID of the routine.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError400 | ApiError401 | ApiError403 | ApiError404 | RoutineResponse
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            user_id=user_id,
        )
    ).parsed
