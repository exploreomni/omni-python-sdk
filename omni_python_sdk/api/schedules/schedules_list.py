from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.schedules_list_content_type import SchedulesListContentType
from ...models.schedules_list_destination import SchedulesListDestination
from ...models.schedules_list_response_200 import SchedulesListResponse200
from ...models.schedules_list_schedule_type import SchedulesListScheduleType
from ...models.schedules_list_sort_direction import SchedulesListSortDirection
from ...models.schedules_list_sort_field import SchedulesListSortField
from ...models.schedules_list_status import SchedulesListStatus
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cursor: str | Unset = "1",
    page_size: int | Unset = 20,
    sort_direction: SchedulesListSortDirection | Unset = "desc",
    sort_field: SchedulesListSortField | Unset = "scheduleName",
    content_type: SchedulesListContentType | Unset = UNSET,
    embed_entity: str | Unset = UNSET,
    destination: SchedulesListDestination | Unset = UNSET,
    identifier: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    q: str | Unset = UNSET,
    schedule_type: SchedulesListScheduleType | Unset = UNSET,
    status: SchedulesListStatus | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    json_sort_field: str | Unset = UNSET
    if not isinstance(sort_field, Unset):
        json_sort_field = sort_field

    params["sortField"] = json_sort_field

    json_content_type: str | Unset = UNSET
    if not isinstance(content_type, Unset):
        json_content_type = content_type

    params["contentType"] = json_content_type

    params["embedEntity"] = embed_entity

    json_destination: str | Unset = UNSET
    if not isinstance(destination, Unset):
        json_destination = destination

    params["destination"] = json_destination

    params["identifier"] = identifier

    json_owner_id: str | Unset = UNSET
    if not isinstance(owner_id, Unset):
        json_owner_id = str(owner_id)
    params["ownerId"] = json_owner_id

    params["q"] = q

    json_schedule_type: str | Unset = UNSET
    if not isinstance(schedule_type, Unset):
        json_schedule_type = schedule_type

    params["scheduleType"] = json_schedule_type

    json_status: str | Unset = UNSET
    if not isinstance(status, Unset):
        json_status = status

    params["status"] = json_status

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/schedules",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | SchedulesListResponse200 | None:
    if response.status_code == 200:
        response_200 = SchedulesListResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | SchedulesListResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = "1",
    page_size: int | Unset = 20,
    sort_direction: SchedulesListSortDirection | Unset = "desc",
    sort_field: SchedulesListSortField | Unset = "scheduleName",
    content_type: SchedulesListContentType | Unset = UNSET,
    embed_entity: str | Unset = UNSET,
    destination: SchedulesListDestination | Unset = UNSET,
    identifier: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    q: str | Unset = UNSET,
    schedule_type: SchedulesListScheduleType | Unset = UNSET,
    status: SchedulesListStatus | Unset = UNSET,
) -> Response[Any | SchedulesListResponse200]:
    """List schedules

    Args:
        cursor (str | Unset): The page number for offset-based pagination. Default: '1'. Example:
            1.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (SchedulesListSortDirection | Unset): The direction to sort results (asc or
            desc). Default: 'desc'. Example: desc.
        sort_field (SchedulesListSortField | Unset): The field to sort results by. Valid values:
            scheduleName, dashboardName, ownerName, lastRun, lastRunStatus. Default: 'scheduleName'.
            Example: scheduleName.
        content_type (SchedulesListContentType | Unset): Filter schedules by content type:
            dashboard, single tile. Example: dashboard.
        embed_entity (str | Unset): Filter schedules by embed entity.
        destination (SchedulesListDestination | Unset): Filter schedules by destination type:
            email, slack, webhook, sftp, s3. Example: email.
        identifier (str | Unset): Filter schedules by the document's unique identifier. Can be
            found in the dashboard's URL after /dashboards/. Example: 12db1a0a.
        owner_id (UUID | Unset): Filter schedules by the owner's user ID. Use the List users
            endpoint to retrieve user IDs. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        q (str | Unset): Search term for filtering schedules by name, dashboard name, or owner
            name (case-insensitive). Example: Weekly.
        schedule_type (SchedulesListScheduleType | Unset): Filter by type: alert, schedule.
            Example: schedule.
        status (SchedulesListStatus | Unset): Filter schedules by delivery status: success, error,
            canceled, none. Example: success.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesListResponse200]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        content_type=content_type,
        embed_entity=embed_entity,
        destination=destination,
        identifier=identifier,
        owner_id=owner_id,
        q=q,
        schedule_type=schedule_type,
        status=status,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = "1",
    page_size: int | Unset = 20,
    sort_direction: SchedulesListSortDirection | Unset = "desc",
    sort_field: SchedulesListSortField | Unset = "scheduleName",
    content_type: SchedulesListContentType | Unset = UNSET,
    embed_entity: str | Unset = UNSET,
    destination: SchedulesListDestination | Unset = UNSET,
    identifier: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    q: str | Unset = UNSET,
    schedule_type: SchedulesListScheduleType | Unset = UNSET,
    status: SchedulesListStatus | Unset = UNSET,
) -> Any | SchedulesListResponse200 | None:
    """List schedules

    Args:
        cursor (str | Unset): The page number for offset-based pagination. Default: '1'. Example:
            1.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (SchedulesListSortDirection | Unset): The direction to sort results (asc or
            desc). Default: 'desc'. Example: desc.
        sort_field (SchedulesListSortField | Unset): The field to sort results by. Valid values:
            scheduleName, dashboardName, ownerName, lastRun, lastRunStatus. Default: 'scheduleName'.
            Example: scheduleName.
        content_type (SchedulesListContentType | Unset): Filter schedules by content type:
            dashboard, single tile. Example: dashboard.
        embed_entity (str | Unset): Filter schedules by embed entity.
        destination (SchedulesListDestination | Unset): Filter schedules by destination type:
            email, slack, webhook, sftp, s3. Example: email.
        identifier (str | Unset): Filter schedules by the document's unique identifier. Can be
            found in the dashboard's URL after /dashboards/. Example: 12db1a0a.
        owner_id (UUID | Unset): Filter schedules by the owner's user ID. Use the List users
            endpoint to retrieve user IDs. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        q (str | Unset): Search term for filtering schedules by name, dashboard name, or owner
            name (case-insensitive). Example: Weekly.
        schedule_type (SchedulesListScheduleType | Unset): Filter by type: alert, schedule.
            Example: schedule.
        status (SchedulesListStatus | Unset): Filter schedules by delivery status: success, error,
            canceled, none. Example: success.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesListResponse200
    """

    return sync_detailed(
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        content_type=content_type,
        embed_entity=embed_entity,
        destination=destination,
        identifier=identifier,
        owner_id=owner_id,
        q=q,
        schedule_type=schedule_type,
        status=status,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = "1",
    page_size: int | Unset = 20,
    sort_direction: SchedulesListSortDirection | Unset = "desc",
    sort_field: SchedulesListSortField | Unset = "scheduleName",
    content_type: SchedulesListContentType | Unset = UNSET,
    embed_entity: str | Unset = UNSET,
    destination: SchedulesListDestination | Unset = UNSET,
    identifier: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    q: str | Unset = UNSET,
    schedule_type: SchedulesListScheduleType | Unset = UNSET,
    status: SchedulesListStatus | Unset = UNSET,
) -> Response[Any | SchedulesListResponse200]:
    """List schedules

    Args:
        cursor (str | Unset): The page number for offset-based pagination. Default: '1'. Example:
            1.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (SchedulesListSortDirection | Unset): The direction to sort results (asc or
            desc). Default: 'desc'. Example: desc.
        sort_field (SchedulesListSortField | Unset): The field to sort results by. Valid values:
            scheduleName, dashboardName, ownerName, lastRun, lastRunStatus. Default: 'scheduleName'.
            Example: scheduleName.
        content_type (SchedulesListContentType | Unset): Filter schedules by content type:
            dashboard, single tile. Example: dashboard.
        embed_entity (str | Unset): Filter schedules by embed entity.
        destination (SchedulesListDestination | Unset): Filter schedules by destination type:
            email, slack, webhook, sftp, s3. Example: email.
        identifier (str | Unset): Filter schedules by the document's unique identifier. Can be
            found in the dashboard's URL after /dashboards/. Example: 12db1a0a.
        owner_id (UUID | Unset): Filter schedules by the owner's user ID. Use the List users
            endpoint to retrieve user IDs. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        q (str | Unset): Search term for filtering schedules by name, dashboard name, or owner
            name (case-insensitive). Example: Weekly.
        schedule_type (SchedulesListScheduleType | Unset): Filter by type: alert, schedule.
            Example: schedule.
        status (SchedulesListStatus | Unset): Filter schedules by delivery status: success, error,
            canceled, none. Example: success.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SchedulesListResponse200]
    """

    kwargs = _get_kwargs(
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        content_type=content_type,
        embed_entity=embed_entity,
        destination=destination,
        identifier=identifier,
        owner_id=owner_id,
        q=q,
        schedule_type=schedule_type,
        status=status,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = "1",
    page_size: int | Unset = 20,
    sort_direction: SchedulesListSortDirection | Unset = "desc",
    sort_field: SchedulesListSortField | Unset = "scheduleName",
    content_type: SchedulesListContentType | Unset = UNSET,
    embed_entity: str | Unset = UNSET,
    destination: SchedulesListDestination | Unset = UNSET,
    identifier: str | Unset = UNSET,
    owner_id: UUID | Unset = UNSET,
    q: str | Unset = UNSET,
    schedule_type: SchedulesListScheduleType | Unset = UNSET,
    status: SchedulesListStatus | Unset = UNSET,
) -> Any | SchedulesListResponse200 | None:
    """List schedules

    Args:
        cursor (str | Unset): The page number for offset-based pagination. Default: '1'. Example:
            1.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (SchedulesListSortDirection | Unset): The direction to sort results (asc or
            desc). Default: 'desc'. Example: desc.
        sort_field (SchedulesListSortField | Unset): The field to sort results by. Valid values:
            scheduleName, dashboardName, ownerName, lastRun, lastRunStatus. Default: 'scheduleName'.
            Example: scheduleName.
        content_type (SchedulesListContentType | Unset): Filter schedules by content type:
            dashboard, single tile. Example: dashboard.
        embed_entity (str | Unset): Filter schedules by embed entity.
        destination (SchedulesListDestination | Unset): Filter schedules by destination type:
            email, slack, webhook, sftp, s3. Example: email.
        identifier (str | Unset): Filter schedules by the document's unique identifier. Can be
            found in the dashboard's URL after /dashboards/. Example: 12db1a0a.
        owner_id (UUID | Unset): Filter schedules by the owner's user ID. Use the List users
            endpoint to retrieve user IDs. Example: 987fcdeb-51a2-43d7-9b56-254415f67890.
        q (str | Unset): Search term for filtering schedules by name, dashboard name, or owner
            name (case-insensitive). Example: Weekly.
        schedule_type (SchedulesListScheduleType | Unset): Filter by type: alert, schedule.
            Example: schedule.
        status (SchedulesListStatus | Unset): Filter schedules by delivery status: success, error,
            canceled, none. Example: success.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SchedulesListResponse200
    """

    return (
        await asyncio_detailed(
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            content_type=content_type,
            embed_entity=embed_entity,
            destination=destination,
            identifier=identifier,
            owner_id=owner_id,
            q=q,
            schedule_type=schedule_type,
            status=status,
        )
    ).parsed
