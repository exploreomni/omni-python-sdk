from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.schedules_remove_recipients_body import SchedulesRemoveRecipientsBody
from ...models.schedules_remove_recipients_response import SchedulesRemoveRecipientsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    schedule_id: UUID,
    *,
    body: SchedulesRemoveRecipientsBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/v1/schedules/{schedule_id}/remove-recipients".format(
            schedule_id=quote(str(schedule_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | SchedulesRemoveRecipientsResponse | None:
    if response.status_code == 200:
        response_200 = SchedulesRemoveRecipientsResponse.from_dict(response.json())

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
) -> Response[Any | SchedulesRemoveRecipientsResponse]:
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
    body: SchedulesRemoveRecipientsBody | Unset = UNSET,
) -> Response[Any | SchedulesRemoveRecipientsResponse]:
    """Remove schedule recipients

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        body (SchedulesRemoveRecipientsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesRemoveRecipientsResponse]
    """

    kwargs = _get_kwargs(
        schedule_id=schedule_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: SchedulesRemoveRecipientsBody | Unset = UNSET,
) -> Any | SchedulesRemoveRecipientsResponse | None:
    """Remove schedule recipients

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        body (SchedulesRemoveRecipientsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesRemoveRecipientsResponse
    """

    return sync_detailed(
        schedule_id=schedule_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: SchedulesRemoveRecipientsBody | Unset = UNSET,
) -> Response[Any | SchedulesRemoveRecipientsResponse]:
    """Remove schedule recipients

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        body (SchedulesRemoveRecipientsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesRemoveRecipientsResponse]
    """

    kwargs = _get_kwargs(
        schedule_id=schedule_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    schedule_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: SchedulesRemoveRecipientsBody | Unset = UNSET,
) -> Any | SchedulesRemoveRecipientsResponse | None:
    """Remove schedule recipients

    Args:
        schedule_id (UUID): The UUID of the scheduled task. Can be found in the schedule's URL
            after /schedules/. Example: 123e4567-e89b-12d3-a456-426614174000.
        body (SchedulesRemoveRecipientsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesRemoveRecipientsResponse
    """

    return (
        await asyncio_detailed(
            schedule_id=schedule_id,
            client=client,
            body=body,
        )
    ).parsed
