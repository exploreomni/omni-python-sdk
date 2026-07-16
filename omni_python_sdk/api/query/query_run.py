from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.query_run_body import QueryRunBody
from ...models.query_run_response import QueryRunResponse
from ...models.query_timeout_response import QueryTimeoutResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: QueryRunBody | Unset = UNSET,
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
        "url": "/api/v1/query/run",
        "params": params,
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | QueryRunResponse | QueryTimeoutResponse | None:
    if response.status_code == 200:
        response_200 = QueryRunResponse.from_dict(response.json())

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

    if response.status_code == 408:
        response_408 = QueryTimeoutResponse.from_dict(response.json())

        return response_408

    if response.status_code == 500:
        response_500 = cast(Any, None)
        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | QueryRunResponse | QueryTimeoutResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRunBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | QueryRunResponse | QueryTimeoutResponse]:
    """Execute a semantic query

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (QueryRunBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryRunResponse | QueryTimeoutResponse]
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
    body: QueryRunBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | QueryRunResponse | QueryTimeoutResponse | None:
    """Execute a semantic query

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (QueryRunBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryRunResponse | QueryTimeoutResponse
    """

    return sync_detailed(
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRunBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | QueryRunResponse | QueryTimeoutResponse]:
    """Execute a semantic query

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (QueryRunBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryRunResponse | QueryTimeoutResponse]
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
    body: QueryRunBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | QueryRunResponse | QueryTimeoutResponse | None:
    """Execute a semantic query

    Args:
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (QueryRunBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryRunResponse | QueryTimeoutResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed
