from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_dbt_exposures_response import ModelsDbtExposuresResponse
from ...models.models_dbt_exposures_sort_direction import (
    ModelsDbtExposuresSortDirection,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ModelsDbtExposuresSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["pageSize"] = page_size

    json_sort_direction: str | Unset = UNSET
    if not isinstance(sort_direction, Unset):
        json_sort_direction = sort_direction

    params["sortDirection"] = json_sort_direction

    params["sortField"] = sort_field

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/dbt-exposures".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsDbtExposuresResponse | None:
    if response.status_code == 200:
        response_200 = ModelsDbtExposuresResponse.from_dict(response.json())

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
) -> Response[Any | ModelsDbtExposuresResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ModelsDbtExposuresSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | ModelsDbtExposuresResponse]:
    """Get dbt exposures

     Returns the dbt exposures for a model, computed on-demand by analyzing which dbt models are
    referenced by dashboards that use this model. Returns exactly one record per dashboard. The exposure
    field is null when a dashboard does not reference any dbt models. Exposure names (exposure.name) may
    contain duplicates when multiple dashboards produce the same name; use deduplication_name for a
    guaranteed-unique value, or use it as a fallback when names collide.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ModelsDbtExposuresSortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsDbtExposuresResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        branch_id=branch_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ModelsDbtExposuresSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Any | ModelsDbtExposuresResponse | None:
    """Get dbt exposures

     Returns the dbt exposures for a model, computed on-demand by analyzing which dbt models are
    referenced by dashboards that use this model. Returns exactly one record per dashboard. The exposure
    field is null when a dashboard does not reference any dbt models. Exposure names (exposure.name) may
    contain duplicates when multiple dashboards produce the same name; use deduplication_name for a
    guaranteed-unique value, or use it as a fallback when names collide.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ModelsDbtExposuresSortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsDbtExposuresResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        branch_id=branch_id,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ModelsDbtExposuresSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | ModelsDbtExposuresResponse]:
    """Get dbt exposures

     Returns the dbt exposures for a model, computed on-demand by analyzing which dbt models are
    referenced by dashboards that use this model. Returns exactly one record per dashboard. The exposure
    field is null when a dashboard does not reference any dbt models. Exposure names (exposure.name) may
    contain duplicates when multiple dashboards produce the same name; use deduplication_name for a
    guaranteed-unique value, or use it as a fallback when names collide.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ModelsDbtExposuresSortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsDbtExposuresResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        cursor=cursor,
        page_size=page_size,
        sort_direction=sort_direction,
        sort_field=sort_field,
        branch_id=branch_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    page_size: int | Unset = 20,
    sort_direction: ModelsDbtExposuresSortDirection | Unset = "desc",
    sort_field: str | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Any | ModelsDbtExposuresResponse | None:
    """Get dbt exposures

     Returns the dbt exposures for a model, computed on-demand by analyzing which dbt models are
    referenced by dashboards that use this model. Returns exactly one record per dashboard. The exposure
    field is null when a dashboard does not reference any dbt models. Exposure names (exposure.name) may
    contain duplicates when multiple dashboards produce the same name; use deduplication_name for a
    guaranteed-unique value, or use it as a fallback when names collide.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        cursor (str | Unset): Cursor for pagination (from previous response nextCursor) Example:
            eyJpZCI6IjEyMzQ1In0.
        page_size (int | Unset): Number of results per page (1-100, integer) Default: 20. Example:
            20.
        sort_direction (ModelsDbtExposuresSortDirection | Unset): Sort direction for results
            Default: 'desc'. Example: desc.
        sort_field (str | Unset): Field to sort results by
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsDbtExposuresResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            cursor=cursor,
            page_size=page_size,
            sort_direction=sort_direction,
            sort_field=sort_field,
            branch_id=branch_id,
        )
    ).parsed
