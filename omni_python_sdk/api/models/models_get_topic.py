from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_get_topic_response import ModelsGetTopicResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    topic_name: str,
    *,
    branch_id: UUID | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/topic/{topic_name}".format(
            model_id=quote(str(model_id), safe=""),
            topic_name=quote(str(topic_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsGetTopicResponse | None:
    if response.status_code == 200:
        response_200 = ModelsGetTopicResponse.from_dict(response.json())

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
) -> Response[Any | ModelsGetTopicResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | ModelsGetTopicResponse]:
    """Get topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsGetTopicResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        topic_name=topic_name,
        branch_id=branch_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
) -> Any | ModelsGetTopicResponse | None:
    """Get topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsGetTopicResponse
    """

    return sync_detailed(
        model_id=model_id,
        topic_name=topic_name,
        client=client,
        branch_id=branch_id,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | ModelsGetTopicResponse]:
    """Get topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsGetTopicResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        topic_name=topic_name,
        branch_id=branch_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
) -> Any | ModelsGetTopicResponse | None:
    """Get topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsGetTopicResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            topic_name=topic_name,
            client=client,
            branch_id=branch_id,
        )
    ).parsed
