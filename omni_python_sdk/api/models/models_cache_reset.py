from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_cache_reset_body import ModelsCacheResetBody
from ...models.models_cache_reset_response import ModelsCacheResetResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    policy_name: str,
    *,
    body: ModelsCacheResetBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/cache_reset/{policy_name}".format(
            model_id=quote(str(model_id), safe=""),
            policy_name=quote(str(policy_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsCacheResetResponse | None:
    if response.status_code == 200:
        response_200 = ModelsCacheResetResponse.from_dict(response.json())

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
) -> Response[Any | ModelsCacheResetResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    policy_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCacheResetBody | Unset = UNSET,
) -> Response[Any | ModelsCacheResetResponse]:
    """Reset cache for policy

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        policy_name (str): Cache policy name Example: daily_refresh.
        body (ModelsCacheResetBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCacheResetResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        policy_name=policy_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    policy_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCacheResetBody | Unset = UNSET,
) -> Any | ModelsCacheResetResponse | None:
    """Reset cache for policy

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        policy_name (str): Cache policy name Example: daily_refresh.
        body (ModelsCacheResetBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCacheResetResponse
    """

    return sync_detailed(
        model_id=model_id,
        policy_name=policy_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    policy_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCacheResetBody | Unset = UNSET,
) -> Response[Any | ModelsCacheResetResponse]:
    """Reset cache for policy

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        policy_name (str): Cache policy name Example: daily_refresh.
        body (ModelsCacheResetBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCacheResetResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        policy_name=policy_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    policy_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsCacheResetBody | Unset = UNSET,
) -> Any | ModelsCacheResetResponse | None:
    """Reset cache for policy

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        policy_name (str): Cache policy name Example: daily_refresh.
        body (ModelsCacheResetBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCacheResetResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            policy_name=policy_name,
            client=client,
            body=body,
        )
    ).parsed
