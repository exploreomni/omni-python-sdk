from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_refresh_hard_refresh import ModelsRefreshHardRefresh
from ...models.models_refresh_response import ModelsRefreshResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    branch_id: UUID | Unset = UNSET,
    hard_refresh: ModelsRefreshHardRefresh | Unset = UNSET,
    schemas: str | Unset = UNSET,
    tables: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    json_hard_refresh: str | Unset = UNSET
    if not isinstance(hard_refresh, Unset):
        json_hard_refresh = hard_refresh

    params["hard_refresh"] = json_hard_refresh

    params["schemas"] = schemas

    params["tables"] = tables

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/refresh".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsRefreshResponse | None:
    if response.status_code == 200:
        response_200 = ModelsRefreshResponse.from_dict(response.json())

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
) -> Response[Any | ModelsRefreshResponse]:
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
    branch_id: UUID | Unset = UNSET,
    hard_refresh: ModelsRefreshHardRefresh | Unset = UNSET,
    schemas: str | Unset = UNSET,
    tables: str | Unset = UNSET,
) -> Response[Any | ModelsRefreshResponse]:
    """Refresh model schema

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-based schema refresh. Required when branch-
            based schema refresh is enabled for the connection. Must not be provided when branch-based
            schema refresh is not enabled. Example: 123e4567-e89b-12d3-a456-426614174001.
        hard_refresh (ModelsRefreshHardRefresh | Unset): When true (the default), performs a hard
            refresh that fully discards and rebuilds the schema model. When false, performs a soft
            refresh that merges newly generated views with the existing model. Must be set to false
            when `schemas` or `tables` filters are provided. Example: false.
        schemas (str | Unset): Optional comma-separated list of schemas to refresh selectively.
            Only the listed schemas are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public,analytics.
        tables (str | Unset): Optional comma-separated list of tables to refresh selectively. Only
            the listed tables are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public.orders,public.customers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsRefreshResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        hard_refresh=hard_refresh,
        schemas=schemas,
        tables=tables,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    hard_refresh: ModelsRefreshHardRefresh | Unset = UNSET,
    schemas: str | Unset = UNSET,
    tables: str | Unset = UNSET,
) -> Any | ModelsRefreshResponse | None:
    """Refresh model schema

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-based schema refresh. Required when branch-
            based schema refresh is enabled for the connection. Must not be provided when branch-based
            schema refresh is not enabled. Example: 123e4567-e89b-12d3-a456-426614174001.
        hard_refresh (ModelsRefreshHardRefresh | Unset): When true (the default), performs a hard
            refresh that fully discards and rebuilds the schema model. When false, performs a soft
            refresh that merges newly generated views with the existing model. Must be set to false
            when `schemas` or `tables` filters are provided. Example: false.
        schemas (str | Unset): Optional comma-separated list of schemas to refresh selectively.
            Only the listed schemas are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public,analytics.
        tables (str | Unset): Optional comma-separated list of tables to refresh selectively. Only
            the listed tables are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public.orders,public.customers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsRefreshResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        branch_id=branch_id,
        hard_refresh=hard_refresh,
        schemas=schemas,
        tables=tables,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    hard_refresh: ModelsRefreshHardRefresh | Unset = UNSET,
    schemas: str | Unset = UNSET,
    tables: str | Unset = UNSET,
) -> Response[Any | ModelsRefreshResponse]:
    """Refresh model schema

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-based schema refresh. Required when branch-
            based schema refresh is enabled for the connection. Must not be provided when branch-based
            schema refresh is not enabled. Example: 123e4567-e89b-12d3-a456-426614174001.
        hard_refresh (ModelsRefreshHardRefresh | Unset): When true (the default), performs a hard
            refresh that fully discards and rebuilds the schema model. When false, performs a soft
            refresh that merges newly generated views with the existing model. Must be set to false
            when `schemas` or `tables` filters are provided. Example: false.
        schemas (str | Unset): Optional comma-separated list of schemas to refresh selectively.
            Only the listed schemas are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public,analytics.
        tables (str | Unset): Optional comma-separated list of tables to refresh selectively. Only
            the listed tables are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public.orders,public.customers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsRefreshResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        hard_refresh=hard_refresh,
        schemas=schemas,
        tables=tables,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    hard_refresh: ModelsRefreshHardRefresh | Unset = UNSET,
    schemas: str | Unset = UNSET,
    tables: str | Unset = UNSET,
) -> Any | ModelsRefreshResponse | None:
    """Refresh model schema

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-based schema refresh. Required when branch-
            based schema refresh is enabled for the connection. Must not be provided when branch-based
            schema refresh is not enabled. Example: 123e4567-e89b-12d3-a456-426614174001.
        hard_refresh (ModelsRefreshHardRefresh | Unset): When true (the default), performs a hard
            refresh that fully discards and rebuilds the schema model. When false, performs a soft
            refresh that merges newly generated views with the existing model. Must be set to false
            when `schemas` or `tables` filters are provided. Example: false.
        schemas (str | Unset): Optional comma-separated list of schemas to refresh selectively.
            Only the listed schemas are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public,analytics.
        tables (str | Unset): Optional comma-separated list of tables to refresh selectively. Only
            the listed tables are reloaded; the rest of the schema model is preserved. Requires
            `hard_refresh=false`. Example: public.orders,public.customers.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsRefreshResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            branch_id=branch_id,
            hard_refresh=hard_refresh,
            schemas=schemas,
            tables=tables,
        )
    ).parsed
