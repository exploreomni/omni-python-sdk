from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.schedules_get_response import SchedulesGetResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    schedule_id: UUID,
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
        "url": "/api/v1/schedules/{schedule_id}".format(
            schedule_id=quote(str(schedule_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | SchedulesGetResponse | None:
    if response.status_code == 200:
        response_200 = SchedulesGetResponse.from_dict(response.json())

        return response_200

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
) -> Response[Any | SchedulesGetResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | SchedulesGetResponse]:
    """Get schedule

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        user_id (UUID | Unset): Membership ID of the user whose access should be checked (org API
            keys only). When provided, the endpoint checks if that user has permission to view the
            schedule. User-scoped API keys cannot use this parameter. Example:
            987fcdeb-51a2-43d7-9b56-254415f67890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesGetResponse]
    """

    kwargs = _get_kwargs(
        schedule_id=schedule_id,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | SchedulesGetResponse | None:
    """Get schedule

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        user_id (UUID | Unset): Membership ID of the user whose access should be checked (org API
            keys only). When provided, the endpoint checks if that user has permission to view the
            schedule. User-scoped API keys cannot use this parameter. Example:
            987fcdeb-51a2-43d7-9b56-254415f67890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesGetResponse
    """

    return sync_detailed(
        schedule_id=schedule_id,
        client=client,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | SchedulesGetResponse]:
    """Get schedule

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        user_id (UUID | Unset): Membership ID of the user whose access should be checked (org API
            keys only). When provided, the endpoint checks if that user has permission to view the
            schedule. User-scoped API keys cannot use this parameter. Example:
            987fcdeb-51a2-43d7-9b56-254415f67890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesGetResponse]
    """

    kwargs = _get_kwargs(
        schedule_id=schedule_id,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    user_id: UUID | Unset = UNSET,
) -> Any | SchedulesGetResponse | None:
    """Get schedule

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        user_id (UUID | Unset): Membership ID of the user whose access should be checked (org API
            keys only). When provided, the endpoint checks if that user has permission to view the
            schedule. User-scoped API keys cannot use this parameter. Example:
            987fcdeb-51a2-43d7-9b56-254415f67890.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesGetResponse
    """

    return (
        await asyncio_detailed(
            schedule_id=schedule_id,
            client=client,
            user_id=user_id,
        )
    ).parsed
