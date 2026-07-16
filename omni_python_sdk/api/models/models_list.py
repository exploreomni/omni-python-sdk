from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_list_include_deleted import ModelsListIncludeDeleted
from ...models.models_list_model_kind import ModelsListModelKind
from ...models.models_list_response import ModelsListResponse
from ...models.models_list_sort_direction import ModelsListSortDirection
from ...models.models_list_sort_field import ModelsListSortField
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    base_model_id: UUID | Unset = UNSET,
    connection_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    include_deleted: ModelsListIncludeDeleted | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    model_kind: ModelsListModelKind | Unset = UNSET,
    name: str | Unset = UNSET,
    page_size: int | Unset = UNSET,
    sort_direction: ModelsListSortDirection | Unset = UNSET,
    sort_field: ModelsListSortField | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_base_model_id: str | Unset = UNSET
    if not isinstance(base_model_id, Unset):
        json_base_model_id = str(base_model_id)
    params["baseModelId"] = json_base_model_id

    json_connection_id: str | Unset = UNSET
    if not isinstance(connection_id, Unset):
        json_connection_id = str(connection_id)
    params["connectionId"] = json_connection_id

    params["cursor"] = cursor

    params["include"] = include

    json_include_deleted: str | Unset = UNSET
    if not isinstance(include_deleted, Unset):
        json_include_deleted = include_deleted

    params["includeDeleted"] = json_include_deleted

    json_model_id: str | Unset = UNSET
    if not isinstance(model_id, Unset):
        json_model_id = str(model_id)
    params["modelId"] = json_model_id

    json_model_kind: str | Unset = UNSET
    if not isinstance(model_kind, Unset):
        json_model_kind = model_kind

    params["modelKind"] = json_model_kind

    params["name"] = name

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    json_sort_field: str | Unset = UNSET
    if not isinstance(sort_field, Unset):
        json_sort_field = sort_field

    params["sortField"] = json_sort_field

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsListResponse | None:
    if response.status_code == 200:
        response_200 = ModelsListResponse.from_dict(response.json())

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
) -> Response[Any | ModelsListResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    base_model_id: UUID | Unset = UNSET,
    connection_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    include_deleted: ModelsListIncludeDeleted | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    model_kind: ModelsListModelKind | Unset = UNSET,
    name: str | Unset = UNSET,
    page_size: int | Unset = UNSET,
    sort_direction: ModelsListSortDirection | Unset = UNSET,
    sort_field: ModelsListSortField | Unset = UNSET,
) -> Response[Any | ModelsListResponse]:
    """List models

    Args:
        base_model_id (UUID | Unset): Filter by base model ID
        connection_id (UUID | Unset): Filter by connection ID
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (e.g., activeBranches)
            Example: activeBranches.
        include_deleted (ModelsListIncludeDeleted | Unset): Include deleted models
        model_id (UUID | Unset): Filter by specific model ID
        model_kind (ModelsListModelKind | Unset): Filter by model kind
        name (str | Unset): Filter by model name
        page_size (int | Unset): Number of results per page Example: 20.
        sort_direction (ModelsListSortDirection | Unset): Sort direction
        sort_field (ModelsListSortField | Unset): Field to sort by

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsListResponse]
    """

    kwargs = _get_kwargs(
        base_model_id=base_model_id,
        connection_id=connection_id,
        cursor=cursor,
        include=include,
        include_deleted=include_deleted,
        model_id=model_id,
        model_kind=model_kind,
        name=name,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    base_model_id: UUID | Unset = UNSET,
    connection_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    include_deleted: ModelsListIncludeDeleted | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    model_kind: ModelsListModelKind | Unset = UNSET,
    name: str | Unset = UNSET,
    page_size: int | Unset = UNSET,
    sort_direction: ModelsListSortDirection | Unset = UNSET,
    sort_field: ModelsListSortField | Unset = UNSET,
) -> Any | ModelsListResponse | None:
    """List models

    Args:
        base_model_id (UUID | Unset): Filter by base model ID
        connection_id (UUID | Unset): Filter by connection ID
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (e.g., activeBranches)
            Example: activeBranches.
        include_deleted (ModelsListIncludeDeleted | Unset): Include deleted models
        model_id (UUID | Unset): Filter by specific model ID
        model_kind (ModelsListModelKind | Unset): Filter by model kind
        name (str | Unset): Filter by model name
        page_size (int | Unset): Number of results per page Example: 20.
        sort_direction (ModelsListSortDirection | Unset): Sort direction
        sort_field (ModelsListSortField | Unset): Field to sort by

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsListResponse
    """

    return sync_detailed(
        client=client,
        base_model_id=base_model_id,
        connection_id=connection_id,
        cursor=cursor,
        include=include,
        include_deleted=include_deleted,
        model_id=model_id,
        model_kind=model_kind,
        name=name,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    base_model_id: UUID | Unset = UNSET,
    connection_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    include_deleted: ModelsListIncludeDeleted | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    model_kind: ModelsListModelKind | Unset = UNSET,
    name: str | Unset = UNSET,
    page_size: int | Unset = UNSET,
    sort_direction: ModelsListSortDirection | Unset = UNSET,
    sort_field: ModelsListSortField | Unset = UNSET,
) -> Response[Any | ModelsListResponse]:
    """List models

    Args:
        base_model_id (UUID | Unset): Filter by base model ID
        connection_id (UUID | Unset): Filter by connection ID
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (e.g., activeBranches)
            Example: activeBranches.
        include_deleted (ModelsListIncludeDeleted | Unset): Include deleted models
        model_id (UUID | Unset): Filter by specific model ID
        model_kind (ModelsListModelKind | Unset): Filter by model kind
        name (str | Unset): Filter by model name
        page_size (int | Unset): Number of results per page Example: 20.
        sort_direction (ModelsListSortDirection | Unset): Sort direction
        sort_field (ModelsListSortField | Unset): Field to sort by

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsListResponse]
    """

    kwargs = _get_kwargs(
        base_model_id=base_model_id,
        connection_id=connection_id,
        cursor=cursor,
        include=include,
        include_deleted=include_deleted,
        model_id=model_id,
        model_kind=model_kind,
        name=name,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    base_model_id: UUID | Unset = UNSET,
    connection_id: UUID | Unset = UNSET,
    cursor: str | Unset = UNSET,
    include: str | Unset = UNSET,
    include_deleted: ModelsListIncludeDeleted | Unset = UNSET,
    model_id: UUID | Unset = UNSET,
    model_kind: ModelsListModelKind | Unset = UNSET,
    name: str | Unset = UNSET,
    page_size: int | Unset = UNSET,
    sort_direction: ModelsListSortDirection | Unset = UNSET,
    sort_field: ModelsListSortField | Unset = UNSET,
) -> Any | ModelsListResponse | None:
    """List models

    Args:
        base_model_id (UUID | Unset): Filter by base model ID
        connection_id (UUID | Unset): Filter by connection ID
        cursor (str | Unset): Cursor for pagination
        include (str | Unset): Comma-separated list of fields to include (e.g., activeBranches)
            Example: activeBranches.
        include_deleted (ModelsListIncludeDeleted | Unset): Include deleted models
        model_id (UUID | Unset): Filter by specific model ID
        model_kind (ModelsListModelKind | Unset): Filter by model kind
        name (str | Unset): Filter by model name
        page_size (int | Unset): Number of results per page Example: 20.
        sort_direction (ModelsListSortDirection | Unset): Sort direction
        sort_field (ModelsListSortField | Unset): Field to sort by

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsListResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            base_model_id=base_model_id,
            connection_id=connection_id,
            cursor=cursor,
            include=include,
            include_deleted=include_deleted,
            model_id=model_id,
            model_kind=model_kind,
            name=name,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
        )
    ).parsed
