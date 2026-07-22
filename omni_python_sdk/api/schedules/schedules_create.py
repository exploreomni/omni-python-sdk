from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.schedules_create_schedules_create_body import SchedulesCreateSchedulesCreateBody
from ...models.schedules_create_schedules_create_response import SchedulesCreateSchedulesCreateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: SchedulesCreateSchedulesCreateBody | Unset = UNSET,
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
        "url": "/api/v1/schedules",
        "params": params,
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | SchedulesCreateSchedulesCreateResponse | None:
    if response.status_code == 200:
        response_200 = SchedulesCreateSchedulesCreateResponse.from_dict(response.json())

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
) -> Response[Any | SchedulesCreateSchedulesCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: SchedulesCreateSchedulesCreateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | SchedulesCreateSchedulesCreateResponse]:
    """Create schedule

     Create a new scheduled delivery for a dashboard. Required fields vary by destinationType (email,
    webhook, sftp, slack). For org API keys, use the userId query parameter to create the schedule on
    behalf of a specific user.

    Args:
        user_id (UUID | Unset): Membership ID of the user who should own the schedule (org API
            keys only). If not provided, the schedule is owned by the API key owner. User-scoped API
            keys cannot use this parameter. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        body (SchedulesCreateSchedulesCreateBody | Unset): Request body for creating a scheduled
            task. Required fields vary by destinationType.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesCreateSchedulesCreateResponse]
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
    body: SchedulesCreateSchedulesCreateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | SchedulesCreateSchedulesCreateResponse | None:
    """Create schedule

     Create a new scheduled delivery for a dashboard. Required fields vary by destinationType (email,
    webhook, sftp, slack). For org API keys, use the userId query parameter to create the schedule on
    behalf of a specific user.

    Args:
        user_id (UUID | Unset): Membership ID of the user who should own the schedule (org API
            keys only). If not provided, the schedule is owned by the API key owner. User-scoped API
            keys cannot use this parameter. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        body (SchedulesCreateSchedulesCreateBody | Unset): Request body for creating a scheduled
            task. Required fields vary by destinationType.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesCreateSchedulesCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: SchedulesCreateSchedulesCreateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | SchedulesCreateSchedulesCreateResponse]:
    """Create schedule

     Create a new scheduled delivery for a dashboard. Required fields vary by destinationType (email,
    webhook, sftp, slack). For org API keys, use the userId query parameter to create the schedule on
    behalf of a specific user.

    Args:
        user_id (UUID | Unset): Membership ID of the user who should own the schedule (org API
            keys only). If not provided, the schedule is owned by the API key owner. User-scoped API
            keys cannot use this parameter. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        body (SchedulesCreateSchedulesCreateBody | Unset): Request body for creating a scheduled
            task. Required fields vary by destinationType.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesCreateSchedulesCreateResponse]
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
    body: SchedulesCreateSchedulesCreateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | SchedulesCreateSchedulesCreateResponse | None:
    """Create schedule

     Create a new scheduled delivery for a dashboard. Required fields vary by destinationType (email,
    webhook, sftp, slack). For org API keys, use the userId query parameter to create the schedule on
    behalf of a specific user.

    Args:
        user_id (UUID | Unset): Membership ID of the user who should own the schedule (org API
            keys only). If not provided, the schedule is owned by the API key owner. User-scoped API
            keys cannot use this parameter. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        body (SchedulesCreateSchedulesCreateBody | Unset): Request body for creating a scheduled
            task. Required fields vary by destinationType.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesCreateSchedulesCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed
